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

"""Join the functionality of all fixtures for API-level integration testing."""

__all__ = [
    "joint_fixture",
    "JointFixture",
    "mongodb_fixture",
    "s3_fixture",
    "kafka_fixture",
    "populated_fixture",
    "PopulatedFixture",
]

import json
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncGenerator

import httpx
import pytest_asyncio
from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.providers.akafka.testutils import KafkaFixture, kafka_fixture
from hexkit.providers.mongodb.testutils import MongoDbFixture  # F401
from hexkit.providers.mongodb.testutils import mongodb_fixture
from hexkit.providers.s3.testutils import S3Fixture, s3_fixture
from pydantic import BaseSettings

from dcs.config import Config
from dcs.container import Container
from dcs.core import models
from dcs.main import get_configured_container, get_rest_api
from tests.fixtures.config import get_config
from tests.fixtures.mock_api.testcontainer import MockAPIContainer

EXAMPLE_FILE = models.DrsObject(
    file_id="examplefile001",
    decrypted_sha256="0677de3685577a06862f226bb1bfa8f889e96e59439d915543929fb4f011d096",
    creation_date=datetime.now().isoformat(),
    decrypted_size=12345,
    decryption_secret_id="some-secret",
)


def get_free_port() -> int:
    """Finds and returns a free port on localhost."""
    sock = socket.socket()
    sock.bind(("", 0))
    return int(sock.getsockname()[1])


class EKSSBaseInjector(BaseSettings):
    """Dynamically inject ekss url"""

    ekss_base_url: str


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    container: Container
    mongodb: MongoDbFixture
    rest_client: httpx.AsyncClient
    s3: S3Fixture
    kafka: KafkaFixture


@pytest_asyncio.fixture
async def joint_fixture(
    mongodb_fixture: MongoDbFixture, s3_fixture: S3Fixture, kafka_fixture: KafkaFixture
) -> AsyncGenerator[JointFixture, None]:
    """A fixture that embeds all other fixtures for API-level integration testing"""

    with MockAPIContainer() as ekss_api:
        # merge configs from different sources with the default one:

        ekss_config = EKSSBaseInjector(ekss_base_url=ekss_api.get_connection_url())

        config = get_config(
            sources=[
                mongodb_fixture.config,
                s3_fixture.config,
                kafka_fixture.config,
                ekss_config,
            ]
        )
        # create a DI container instance:translators
        async with get_configured_container(config=config) as container:
            container.wire(modules=["dcs.adapters.inbound.fastapi_.routes"])

            # create storage entities:
            await s3_fixture.populate_buckets(buckets=[config.outbox_bucket])

            api = get_rest_api(config=config)
            port = get_free_port()
            # setup an API test client:
            async with httpx.AsyncClient(
                app=api, base_url=f"http://localhost:{port}"
            ) as rest_client:
                yield JointFixture(
                    config=config,
                    container=container,
                    mongodb=mongodb_fixture,
                    rest_client=rest_client,
                    s3=s3_fixture,
                    kafka=kafka_fixture,
                )


@dataclass
class PopulatedFixture:
    """Returned by `populated_fixture()`."""

    drs_id: str
    example_file: models.DrsObject
    joint_fixture: JointFixture


@pytest_asyncio.fixture
async def populated_fixture(
    joint_fixture: JointFixture,
) -> AsyncGenerator[PopulatedFixture, None]:
    """Prepopulate state for an existing DRS object"""
    # publish an event to register a new file for download:
    files_to_register_event = event_schemas.FileInternallyRegistered(
        file_id=EXAMPLE_FILE.file_id,
        upload_date=EXAMPLE_FILE.creation_date,
        decrypted_size=EXAMPLE_FILE.decrypted_size,
        decrypted_sha256=EXAMPLE_FILE.decrypted_sha256,
        encrypted_part_size=1,
        encrypted_parts_md5=["some", "checksum"],
        encrypted_parts_sha256=["some", "checksum"],
        content_offset=1234,
        decryption_secret_id="some-secret",
    )
    await joint_fixture.kafka.publish_event(
        payload=json.loads(files_to_register_event.json()),
        type_=joint_fixture.config.files_to_register_type,
        topic=joint_fixture.config.files_to_register_topic,
    )

    # consume the event:
    event_subscriber = await joint_fixture.container.event_subscriber()
    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.file_registered_event_topic
    ) as recorder:
        await event_subscriber.run(forever=False)

    # check that an event informing about the newly registered file was published:
    assert len(recorder.recorded_events) == 1
    assert (
        recorder.recorded_events[0].type_
        == joint_fixture.config.file_registered_event_type
    )
    file_registered_event = event_schemas.FileRegisteredForDownload(
        **recorder.recorded_events[0].payload
    )
    assert file_registered_event.file_id == EXAMPLE_FILE.file_id
    assert file_registered_event.decrypted_sha256 == EXAMPLE_FILE.decrypted_sha256
    assert file_registered_event.upload_date == EXAMPLE_FILE.creation_date

    yield PopulatedFixture(
        drs_id=EXAMPLE_FILE.file_id,
        example_file=EXAMPLE_FILE,
        joint_fixture=joint_fixture,
    )
