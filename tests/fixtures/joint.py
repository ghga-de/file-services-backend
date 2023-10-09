# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Bundle test fixtures together"""
import base64
import os
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import httpx
import pytest_asyncio
from ghga_service_commons.api.testing import AsyncTestClient
from ghga_service_commons.utils.crypt import (
    KeyPair,
    encode_key,
    encrypt,
    generate_key_pair,
)
from ghga_service_commons.utils.simple_token import generate_token_and_hash
from hexkit.providers.akafka.testutils import KafkaFixture, kafka_fixture  # noqa: F401

from fis.config import Config, ServiceConfig
from fis.container import Container
from fis.core.models import FileUploadMetadata, FileUploadMetadataEncrypted
from fis.main import get_configured_container, get_rest_api
from tests.fixtures.config import get_config

TEST_PAYLOAD = FileUploadMetadata(
    file_id="abc",
    object_id="happy_little_object",
    part_size=16 * 1024**2,
    unencrypted_size=50 * 1024**2,
    encrypted_size=50 * 1024**2 + 128,
    file_secret=base64.b64encode(os.urandom(32)).decode(),
    unencrypted_checksum="def",
    encrypted_md5_checksums=["a", "b", "c"],
    encrypted_sha256_checksums=["a", "b", "c"],
)


@dataclass
class JointFixture:
    """Holds generated test keypair and configured container"""

    config: Config
    container: Container
    keypair: KeyPair
    token: str
    payload: FileUploadMetadata
    encrypted_payload: FileUploadMetadataEncrypted
    kafka: KafkaFixture
    rest_client: httpx.AsyncClient


@pytest_asyncio.fixture
async def joint_fixture(
    kafka_fixture: KafkaFixture,  # noqa: F811
) -> AsyncGenerator[JointFixture, None]:
    """Generate keypair for testing and setup container with updated config"""
    keypair = generate_key_pair()
    private_key = encode_key(key=keypair.private)

    token, token_hash = generate_token_and_hash()

    service_config = ServiceConfig(
        source_bucket_id="test-staging",
        private_key=private_key,
        token_hashes=[token_hash],
    )
    config = get_config(sources=[kafka_fixture.config, service_config])
    container = get_configured_container(config=config)
    container.wire(
        modules=[
            "fis.adapters.inbound.fastapi_.http_authorization",
            "fis.adapters.inbound.fastapi_.routes",
        ]
    )

    encrypted_payload = FileUploadMetadataEncrypted(
        payload=encrypt(
            data=TEST_PAYLOAD.json(),
            key=keypair.public,
        )
    )

    api = get_rest_api(config=config)

    async with AsyncTestClient(app=api) as rest_client:
        yield JointFixture(
            config=config,
            container=container,
            keypair=keypair,
            payload=TEST_PAYLOAD,
            encrypted_payload=encrypted_payload,
            token=token,
            kafka=kafka_fixture,
            rest_client=rest_client,
        )
