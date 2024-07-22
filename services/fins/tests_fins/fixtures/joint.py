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

"""Join the functionality of all fixtures for API-level integration testing."""

__all__ = [
    "joint_fixture",
    "JointFixture",
    "kafka_container_fixture",
    "kafka_fixture",
    "mongodb_container_fixture",
    "mongodb_fixture",
]
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import httpx
import pytest_asyncio
from ghga_service_commons.api.testing import AsyncTestClient
from hexkit.providers.akafka import KafkaEventSubscriber, KafkaOutboxSubscriber
from hexkit.providers.akafka.testutils import (
    KafkaFixture,
    kafka_container_fixture,
    kafka_fixture,
)
from hexkit.providers.mongodb import MongoDbDaoFactory
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,
    mongodb_fixture,
)

from fins.adapters.inbound.dao import get_file_information_dao
from fins.config import Config
from fins.inject import (
    prepare_core,
    prepare_event_subscriber,
    prepare_outbox_subscriber,
    prepare_rest_app,
)
from fins.ports.inbound.dao import FileInformationDaoPort
from fins.ports.inbound.information_service import InformationServicePort
from tests_fins.fixtures.config import get_config


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    information_service: InformationServicePort
    file_information_dao: FileInformationDaoPort
    rest_client: httpx.AsyncClient
    event_subscriber: KafkaEventSubscriber
    outbox_subscriber: KafkaOutboxSubscriber
    mongodb: MongoDbFixture
    kafka: KafkaFixture


@pytest_asyncio.fixture
async def joint_fixture(
    mongodb: MongoDbFixture,
    kafka: KafkaFixture,
) -> AsyncGenerator[JointFixture, None]:
    """A fixture that embeds all other fixtures for API-level integration testing"""
    config = get_config(
        sources=[
            mongodb.config,
            kafka.config,
        ]
    )

    dao_factory = MongoDbDaoFactory(config=config)
    file_information_dao = await get_file_information_dao(dao_factory=dao_factory)

    # prepare everything except the outbox subscriber
    async with (
        prepare_core(config=config) as information_service,
        prepare_rest_app(
            config=config, information_service_override=information_service
        ) as app,
        prepare_event_subscriber(
            config=config, information_service_override=information_service
        ) as event_subscriber,
        prepare_outbox_subscriber(
            config=config,
            information_service_override=information_service,
        ) as outbox_subscriber,
        AsyncTestClient(app=app) as rest_client,
    ):
        yield JointFixture(
            config=config,
            information_service=information_service,
            file_information_dao=file_information_dao,
            rest_client=rest_client,
            event_subscriber=event_subscriber,
            outbox_subscriber=outbox_subscriber,
            mongodb=mongodb,
            kafka=kafka,
        )
