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

"""Join the functionality of all fixtures for API-level integration testing."""

__all__ = ["JointFixture", "joint_fixture"]

from collections.abc import AsyncGenerator
from dataclasses import dataclass

import httpx
import pytest_asyncio
from ghga_service_commons.api.testing import AsyncTestClient
from ghga_service_commons.utils.multinode_storage import (
    S3ObjectStorageNodeConfig,
    S3ObjectStoragesConfig,
)
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.s3.testutils import S3Fixture
from jwcrypto.jwk import JWK

from tests_ucs.fixtures.config import get_config
from tests_ucs.fixtures.utils import generate_token_signing_keys
from ucs.config import Config
from ucs.inject import prepare_core, prepare_rest_app
from ucs.ports.inbound.controller import UploadControllerPort


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    upload_controller: UploadControllerPort
    rest_client: httpx.AsyncClient
    mongodb: MongoDbFixture
    kafka: KafkaFixture
    s3: S3Fixture
    bucket_id: str
    jwk: JWK


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    mongodb: MongoDbFixture,
    kafka: KafkaFixture,
    s3: S3Fixture,
) -> AsyncGenerator[JointFixture, None]:
    """A fixture that embeds all other fixtures for API-level integration testing."""
    jwk = generate_token_signing_keys()
    auth_key = jwk.export(private_key=False)

    bucket_id = "test-inbox"
    node_config = S3ObjectStorageNodeConfig(bucket=bucket_id, credentials=s3.config)
    object_storages_config = S3ObjectStoragesConfig(
        object_storages={"test": node_config}
    )

    # merge configs from different sources with the default one:
    config = get_config(
        sources=[mongodb.config, kafka.config, object_storages_config],
        auth_key=auth_key,
    )

    await s3.populate_buckets([bucket_id])

    # Assemble joint fixture with config injection
    async with (
        prepare_core(config=config) as upload_controller,
        prepare_rest_app(config=config, core_override=upload_controller) as app,
        AsyncTestClient(app=app) as rest_client,
    ):
        yield JointFixture(
            config=config,
            upload_controller=upload_controller,
            rest_client=rest_client,
            mongodb=mongodb,
            kafka=kafka,
            s3=s3,
            bucket_id=bucket_id,
            jwk=jwk,
        )
