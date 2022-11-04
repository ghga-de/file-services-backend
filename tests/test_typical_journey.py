# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

"""Tests typical user journeys"""

from datetime import datetime
from time import sleep
from unittest.mock import AsyncMock

import pytest
import requests
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject, S3Fixture

from dcs.adapters.outbound.dao import DrsObjectDaoConstructor
from dcs.core import models
from dcs.core.data_repository import DataRepository, DataRepositoryConfig
from dcs.ports.inbound.data_repository import DataRepositoryPort
from dcs.ports.outbound.event_broadcast import DrsEventBroadcasterPort

EXAMPLE_FILE = models.FileToRegister(
    file_id="examplefile001",
    decrypted_sha256="0677de3685577a06862f226bb1bfa8f889e96e59439d915543929fb4f011d096",
    creation_date=datetime.now(),
    size=12345,
)


@pytest.mark.asyncio
async def test_happy(
    s3_fixture: S3Fixture,  # noqa: F811
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Simulates a typical, successful API journey."""

    # Setup DataRepository:
    config = DataRepositoryConfig(
        outbox_bucket="test-outbox",
        drs_server_uri="http://localhost:1234/",  # a dummy, should not be requested
        retry_access_after=1,
    )
    await s3_fixture.populate_buckets(buckets=[config.outbox_bucket])
    drs_object_dao = await DrsObjectDaoConstructor.construct(
        dao_factory=mongodb_fixture.dao_factory
    )
    event_broadcaster = AsyncMock(spec=DrsEventBroadcasterPort)
    data_repo = DataRepository(
        config=config,
        drs_object_dao=drs_object_dao,
        object_storage=s3_fixture.storage,
        event_broadcaster=event_broadcaster,
    )

    # register new file for download:
    await data_repo.register_new_file(file=EXAMPLE_FILE)

    # check for registration related event and get the newly inserted DRS object:
    event_broadcaster.new_drs_object_registered.assert_awaited_once()
    drs_object: models.DrsObjectWithUri = (
        event_broadcaster.new_drs_object_registered.await_args.kwargs["drs_object"]
    )
    assert drs_object.file_id == EXAMPLE_FILE.file_id

    # request access to the newly registered file:
    try:
        await data_repo.access_drs_object(drs_id=drs_object.id)
    except DataRepositoryPort.RetryAccessLaterError as error:
        retry_after = error.retry_after
        assert retry_after == config.retry_access_after
    else:
        raise AssertionError("Expected RetryAccessLaterError.")

    # wait for the specified time:
    sleep(retry_after)

    # place the requested file into the outbox bucket (it is not important here that
    # the file content matches the announced decrypted_sha256 checksum):
    file_object = file_fixture.copy(
        update={"bucket_id": config.outbox_bucket, "object_id": EXAMPLE_FILE.file_id}
    )
    await s3_fixture.populate_file_objects([file_object])

    # retry the access request:
    drs_object_with_access = await data_repo.access_drs_object(drs_id=drs_object.id)

    # download file bytes:
    response = requests.get(drs_object_with_access.access_url)
    response.raise_for_status()
    assert response.content == file_object.content
