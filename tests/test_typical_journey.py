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
from fastapi import status
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject

from dcs.adapters.outbound.dao import DrsObjectDaoConstructor
from dcs.core import models
from dcs.core.data_repository import DataRepository, DataRepositoryConfig
from dcs.ports.outbound.event_broadcast import DrsEventBroadcasterPort
from tests.fixtures.joint import joint_fixture  # noqa F811
from tests.fixtures.joint import JointFixture

EXAMPLE_FILE = models.FileToRegister(
    file_id="examplefile001",
    decrypted_sha256="0677de3685577a06862f226bb1bfa8f889e96e59439d915543929fb4f011d096",
    creation_date=datetime.now(),
    size=12345,
)


@pytest.mark.asyncio
async def test_happy(
    joint_fixture: JointFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Simulates a typical, successful API journey."""

    # Setup DataRepository:
    config = DataRepositoryConfig(
        outbox_bucket="test-outbox",
        drs_server_uri="http://localhost:1234/",  # a dummy, should not be requested
        retry_access_after=1,
    )
    await joint_fixture.s3.populate_buckets(buckets=[config.outbox_bucket])
    drs_object_dao = await DrsObjectDaoConstructor.construct(
        dao_factory=joint_fixture.mongodb.dao_factory
    )
    event_broadcaster = AsyncMock(spec=DrsEventBroadcasterPort)
    data_repo = DataRepository(
        config=config,
        drs_object_dao=drs_object_dao,
        object_storage=joint_fixture.s3.storage,
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
    response = await joint_fixture.rest_client.get(f"/objects/{drs_object.id}")
    assert response.status_code == status.HTTP_202_ACCEPTED
    retry_after = int(response.headers["Retry-After"])

    # place the requested file into the outbox bucket (it is not important here that
    # the file content matches the announced decrypted_sha256 checksum):
    file_object = file_fixture.copy(
        update={"bucket_id": config.outbox_bucket, "object_id": drs_object.file_id}
    )
    await joint_fixture.s3.populate_file_objects([file_object])

    # wait for the specified time:
    sleep(retry_after)

    # retry the access request:
    drs_object_response = await joint_fixture.rest_client.get(
        f"/objects/{drs_object.id}"
    )

    # download file bytes:
    dowloaded_file = requests.get(drs_object_response.json()["access_url"])
    dowloaded_file.raise_for_status()
    assert dowloaded_file.content == file_object.content
