# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Module hosting the dependency injection container."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext

from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.providers.akafka.provider import KafkaEventPublisher, KafkaEventSubscriber
from hexkit.providers.mongodb import MongoDbDaoFactory
from hexkit.providers.mongokafka import PersistentKafkaPublisher

from irs.adapters.inbound.event_sub import EventSubTranslator
from irs.adapters.outbound.dao import get_fingerprint_dao, get_staging_object_dao
from irs.adapters.outbound.event_pub import EventPublisher
from irs.config import Config
from irs.core.interrogator import Interrogator
from irs.core.storage_inspector import StagingInspector
from irs.ports.inbound.interrogator import InterrogatorPort


@asynccontextmanager
async def get_persistent_publisher(
    config: Config, dao_factory: MongoDbDaoFactory | None = None
) -> AsyncGenerator[PersistentKafkaPublisher]:
    """Construct and return a PersistentKafkaPublisher."""
    async with (
        (
            nullcontext(dao_factory)
            if dao_factory
            else MongoDbDaoFactory.construct(config=config)
        ) as _dao_factory,
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=_dao_factory,
            compacted_topics={config.file_interrogations_topic},
            collection_name="irsPersistedEvents",
        ) as persistent_publisher,
    ):
        yield persistent_publisher


@asynccontextmanager
async def prepare_core(*, config: Config) -> AsyncGenerator[InterrogatorPort]:
    """Constructs and initializes all core components and their outbound dependencies."""
    async with (
        MongoDbDaoFactory.construct(config=config) as dao_factory,
        get_persistent_publisher(
            config=config, dao_factory=dao_factory
        ) as persistent_pub_provider,
    ):
        fingerprint_dao = await get_fingerprint_dao(dao_factory=dao_factory)
        staging_object_dao = await get_staging_object_dao(dao_factory=dao_factory)
        event_publisher = EventPublisher(
            config=config, provider=persistent_pub_provider
        )
        object_storages = S3ObjectStorages(config=config)
        yield Interrogator(
            config=config,
            event_publisher=event_publisher,
            fingerprint_dao=fingerprint_dao,
            staging_object_dao=staging_object_dao,
            object_storages=object_storages,
        )


def prepare_core_with_override(
    *, config: Config, interrogator_override: InterrogatorPort | None = None
):
    """Resolve the interrogator context manager based on config and override (if any)."""
    return (
        nullcontext(interrogator_override)
        if interrogator_override
        else prepare_core(config=config)
    )


@asynccontextmanager
async def prepare_event_subscriber(
    *,
    config: Config,
    interrogator_override: InterrogatorPort | None = None,
) -> AsyncGenerator[KafkaEventSubscriber]:
    """Construct and initialize an event subscriber with all its dependencies.

    By default, the core dependencies are automatically prepared but you can also
    provide them using the interrogator_override parameter.
    """
    async with prepare_core_with_override(
        config=config, interrogator_override=interrogator_override
    ) as interrogator:
        event_sub_translator = EventSubTranslator(
            interrogator=interrogator,
            config=config,
        )

        async with (
            KafkaEventPublisher.construct(config=config) as dlq_publisher,
            KafkaEventSubscriber.construct(
                config=config,
                translator=event_sub_translator,
                dlq_publisher=dlq_publisher,
            ) as event_subscriber,
        ):
            yield event_subscriber


@asynccontextmanager
async def prepare_storage_inspector(*, config: Config):
    """Alternative to prepare_core for storage inspection CLI command without Kafka."""
    object_storages = S3ObjectStorages(config=config)
    async with MongoDbDaoFactory.construct(config=config) as dao_factory:
        staging_object_dao = await get_staging_object_dao(dao_factory=dao_factory)
        yield StagingInspector(
            config=config,
            staging_object_dao=staging_object_dao,
            object_storages=object_storages,
        )
