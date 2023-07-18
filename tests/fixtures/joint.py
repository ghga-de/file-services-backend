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
    "cleanup_fixture",
    "CleanupFixture",
    "file_fixture",
    "joint_fixture",
    "JointFixture",
    "mongodb_fixture",
    "s3_fixture",
    "kafka_fixture",
    "populated_fixture",
    "PopulatedFixture",
    "get_work_order_token",
]

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import AsyncGenerator

import httpx
import pytest_asyncio
from ghga_event_schemas import pydantic_ as event_schemas
from ghga_service_commons.api.testing import AsyncTestClient
from ghga_service_commons.utils import jwt_helpers, utc_dates
from ghga_service_commons.utils.crypt import encode_key, generate_key_pair
from hexkit.providers.testing.fixtures import (
    KafkaFixture,
    MongoDbFixture,
    S3Fixture,
    file_fixture,
    get_fixture,
    temp_file_object,
)
from pydantic import BaseSettings

from dcs.config import Config, WorkOrderTokenConfig
from dcs.container import Container, auth_provider
from dcs.core import auth_policies, models
from dcs.main import get_configured_container, get_rest_api
from dcs.ports.outbound.dao import DrsObjectDaoPort
from tests.fixtures.config import get_config
from tests.fixtures.mock_api.testcontainer import MockAPIContainer

EXAMPLE_FILE = models.AccessTimeDrsObject(
    file_id="examplefile001",
    object_id="object001",
    decrypted_sha256="0677de3685577a06862f226bb1bfa8f889e96e59439d915543929fb4f011d096",
    creation_date=datetime.now().isoformat(),
    decrypted_size=12345,
    decryption_secret_id="some-secret",
    last_accessed=utc_dates.now_as_utc(),
)

SignedToken = str
PubKey = str

mongodb_fixture = get_fixture(MongoDbFixture)
s3_fixture = get_fixture(S3Fixture)
kafka_fixture = get_fixture(KafkaFixture)


def get_work_order_token(
    file_id: str,
    valid_seconds: int = 30,
) -> tuple[SignedToken, PubKey]:
    """Generate work order token for testing"""

    # we don't need the actual user pubkey
    user_pubkey = encode_key(generate_key_pair().public)
    # generate minimal test token
    wot = auth_policies.WorkOrderContext(
        type="download",
        file_id=file_id,
        user_id="007",
        user_public_crypt4gh_key=user_pubkey,
        full_user_name="John Doe",
        email="john.doe@test.com",
    )
    claims = wot.dict()

    jwk = jwt_helpers.generate_jwk()

    signed_token = jwt_helpers.sign_and_serialize_token(
        claims=claims, key=jwk, valid_seconds=valid_seconds
    )
    signing_pubkey = jwk.export_public()
    return signed_token, signing_pubkey


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

    auth_key = jwt_helpers.generate_jwk().export(private_key=False)
    with MockAPIContainer() as ekss_api:
        # merge configs from different sources with the default one:
        auth_config = WorkOrderTokenConfig(auth_key=auth_key)
        ekss_config = EKSSBaseInjector(ekss_base_url=ekss_api.get_connection_url())

        config = get_config(
            sources=[
                mongodb_fixture.config,
                s3_fixture.config,
                kafka_fixture.config,
                ekss_config,
                auth_config,
            ]
        )
        # create a DI container instance:translators
        async with get_configured_container(config=config) as container:
            container.wire(
                modules=[
                    "dcs.adapters.inbound.fastapi_.routes",
                    "dcs.adapters.inbound.fastapi_.http_authorization",
                ]
            )

            # create storage entities:
            await s3_fixture.populate_buckets(buckets=[config.outbox_bucket])

            api = get_rest_api(config=config)
            # setup an API test client:
            async with AsyncTestClient(app=api) as rest_client:
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
    object_id: str
    example_file: models.AccessTimeDrsObject
    joint_fixture: JointFixture


@pytest_asyncio.fixture
async def populated_fixture(
    joint_fixture: JointFixture,
) -> AsyncGenerator[PopulatedFixture, None]:
    """Prepopulate state for an existing DRS object"""
    # publish an event to register a new file for download:
    files_to_register_event = event_schemas.FileInternallyRegistered(
        file_id=EXAMPLE_FILE.file_id,
        object_id=EXAMPLE_FILE.object_id,
        bucket_id=joint_fixture.config.outbox_bucket,
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

    # get the object id that was generated upon event consumption
    dao = await joint_fixture.container.drs_object_dao()
    drs_object = await dao.get_by_id(EXAMPLE_FILE.file_id)
    object_id = drs_object.object_id

    # generate work order token
    work_order_token, pubkey = get_work_order_token(  # noqa: F405
        file_id=EXAMPLE_FILE.file_id,
        valid_seconds=120,
    )

    # modify default headers and patch signing pubkey
    joint_fixture.rest_client.headers = httpx.Headers(
        {"Authorization": f"Bearer {work_order_token}"}
    )
    auth_provider_override = auth_provider(config=WorkOrderTokenConfig(auth_key=pubkey))
    joint_fixture.container.auth_provider.override(auth_provider_override)

    yield PopulatedFixture(
        drs_id=EXAMPLE_FILE.file_id,
        object_id=object_id,
        example_file=EXAMPLE_FILE,
        joint_fixture=joint_fixture,
    )


@dataclass
class CleanupFixture:
    """Fixture for cleanup test with DAO and test files"""

    mongodb_dao: DrsObjectDaoPort
    joint_fixture: JointFixture
    cached_id: str
    expired_id: str


@pytest_asyncio.fixture
async def cleanup_fixture(
    joint_fixture: JointFixture,
) -> AsyncGenerator[CleanupFixture, None]:
    """Set up state for and populate CleanupFixture"""

    # create AccessTimeDrsObjects for valid cached and expired cached file
    test_file_cached = EXAMPLE_FILE.copy(deep=True)
    test_file_cached.file_id = "cached"
    test_file_cached.object_id = "cached"
    test_file_cached.last_accessed = utc_dates.now_as_utc()

    test_file_expired = EXAMPLE_FILE.copy(deep=True)
    test_file_expired.file_id = "expired"
    test_file_expired.object_id = "expired"
    test_file_expired.last_accessed = utc_dates.now_as_utc() - timedelta(
        days=joint_fixture.config.cache_timeout
    )

    # populate DB entries
    mongodb_dao = await joint_fixture.mongodb.dao_factory.get_dao(
        name="drs_objects",
        dto_model=models.AccessTimeDrsObject,
        id_field="file_id",
    )
    await mongodb_dao.insert(test_file_cached)
    await mongodb_dao.insert(test_file_expired)

    # populate storage
    with temp_file_object(
        bucket_id=joint_fixture.config.outbox_bucket,
        object_id=test_file_cached.object_id,
    ) as cached_file:
        with temp_file_object(
            bucket_id=joint_fixture.config.outbox_bucket,
            object_id=test_file_expired.object_id,
        ) as expired_file:
            await joint_fixture.s3.populate_file_objects([cached_file, expired_file])

    yield CleanupFixture(
        mongodb_dao=mongodb_dao,
        joint_fixture=joint_fixture,
        cached_id=test_file_cached.file_id,
        expired_id=test_file_expired.file_id,
    )
