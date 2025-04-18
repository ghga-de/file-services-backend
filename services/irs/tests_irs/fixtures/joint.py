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

"""Provides multiple fixtures in one spot"""

from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest_asyncio
from ghga_service_commons.utils.multinode_storage import (
    S3ObjectStorageNodeConfig,
    S3ObjectStoragesConfig,
)
from hexkit.providers.akafka import KafkaEventSubscriber
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.s3.testutils import S3Fixture

from irs.config import Config
from irs.inject import (
    prepare_core,
    prepare_event_subscriber,
)
from irs.ports.inbound.interrogator import InterrogatorPort
from tests_irs.fixtures.config import get_config
from tests_irs.fixtures.keypair_fixtures import KeypairFixture

FILE_SIZE = 50 * 1024**2
INBOX_BUCKET_ID = "test-inbox"
STAGING_BUCKET_ID = "test-staging"


@dataclass
class EndpointAliases:
    """Container class for endpoint aliases to be used by test code"""

    node1: str = "test"
    node2: str = "test2"


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    event_subscriber: KafkaEventSubscriber
    interrogator: InterrogatorPort
    kafka: KafkaFixture
    keypair: KeypairFixture
    mongodb: MongoDbFixture
    s3: S3Fixture
    endpoint_aliases: EndpointAliases


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    keypair_fixture: KeypairFixture,
    kafka: KafkaFixture,
    mongodb: MongoDbFixture,
    s3: S3Fixture,
) -> AsyncGenerator[JointFixture, None]:
    """A fixture that embeds all other fixtures for integration testing"""
    node_config = S3ObjectStorageNodeConfig(
        bucket=STAGING_BUCKET_ID, credentials=s3.config
    )

    endpoint_aliases = EndpointAliases()

    object_storage_config = S3ObjectStoragesConfig(
        object_storages={
            endpoint_aliases.node1: node_config,
        }
    )
    config = get_config(
        sources=[kafka.config, mongodb.config, object_storage_config],
        kafka_enable_dlq=True,
    )

    # Create joint_fixture using the inject module
    async with (
        prepare_core(config=config) as interrogator,
        prepare_event_subscriber(
            config=config, interrogator_override=interrogator
        ) as event_subscriber,
    ):
        yield JointFixture(
            config=config,
            event_subscriber=event_subscriber,
            interrogator=interrogator,
            kafka=kafka,
            keypair=keypair_fixture,
            mongodb=mongodb,
            s3=s3,
            endpoint_aliases=endpoint_aliases,
        )
