# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
from contextlib import asynccontextmanager

from fastapi import FastAPI
from ghga_service_commons.utils.context import asyncnullcontext
from hexkit.providers.akafka import KafkaOutboxSubscriber
from hexkit.providers.mongodb import MongoDbDaoFactory

from fins.adapters.inbound.event_sub import (
    InformationDeletionRequestedListener,
    InformationRegistrationListener,
)
from fins.adapters.inbound.fastapi_ import dummies
from fins.adapters.inbound.fastapi_.configure import get_configured_app
from fins.config import Config
from fins.core.information_service import InformationService
from fins.ports.inbound.information_service import InformationServicePort


@asynccontextmanager
async def prepare_core(
    *, config: Config
) -> AsyncGenerator[InformationServicePort, None]:
    """Constructs and initializes all core components and their outbound dependencies."""
    dao_factory = MongoDbDaoFactory(config=config)

    information_service = InformationService()
    yield information_service


def prepare_core_with_override(
    *,
    config: Config,
    core_override: InformationServicePort | None = None,
):
    """Resolve the prepare_core context manager based on config and override (if any)."""
    return (
        asyncnullcontext(core_override)
        if core_override
        else prepare_core(config=config)
    )


@asynccontextmanager
async def prepare_outbox_subscriber(
    *,
    config: Config,
    core_override: InformationServicePort | None = None,
) -> AsyncGenerator[KafkaOutboxSubscriber, None]:
    """Construct and initialize an event subscriber with all its dependencies.
    By default, the core dependencies are automatically prepared but you can also
    provide them using the core_override parameter.
    """
    async with prepare_core_with_override(
        config=config, core_override=core_override
    ) as information_service:
        outbox_translators = [
            cls(config=config, information_service=information_service)
            for cls in (
                InformationDeletionRequestedListener,
                InformationRegistrationListener,
            )
        ]
        async with KafkaOutboxSubscriber.construct(
            config=config, translators=outbox_translators
        ) as kafka_outbox_subscriber:
            yield kafka_outbox_subscriber


@asynccontextmanager
async def prepare_rest_app(
    *,
    config: Config,
    core_override: InformationServicePort | None = None,
) -> AsyncGenerator[FastAPI, None]:
    """Construct and initialize an REST API app along with all its dependencies.
    By default, the core dependencies are automatically prepared but you can also
    provide them using the core_override parameter.
    """
    app = get_configured_app(config=config)

    async with prepare_core_with_override(
        config=config, core_override=core_override
    ) as information_service:
        app.dependency_overrides[dummies.information_service] = (
            lambda: information_service
        )
        yield app
