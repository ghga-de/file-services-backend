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

"""Tests edge cases not covered by the typical journey test."""

from unittest.mock import AsyncMock

import pytest
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject, S3Fixture

from dcs.adapters.outbound.dao import DrsObjectDaoConstructor
from dcs.core.data_repository import DataRepository, DataRepositoryConfig
from dcs.ports.inbound.data_repository import DataRepositoryPort
from dcs.ports.outbound.event_broadcast import DrsEventBroadcasterPort


@pytest.mark.asyncio
async def test_access_non_existing(
    s3_fixture: S3Fixture,  # noqa: F811
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Checks that requesting access to a non-existing DRS object fails with the
    expected exception."""

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

    # request access to non existing DRS object:
    with pytest.raises(DataRepositoryPort.DrsObjectNotFoundError):
        await data_repo.access_drs_object(drs_id="my-non-existing-id")
