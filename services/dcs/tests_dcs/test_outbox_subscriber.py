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

"""Tests for functionality related to the outbox subscriber."""

from unittest.mock import AsyncMock

import pytest
from dcs.adapters.inbound.idempotent import (
    assert_record_is_new,
    make_record_from_update,
)
from dcs.adapters.outbound.dao import get_file_deletion_requested_dao
from dcs.core import models
from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.correlation import get_correlation_id
from hexkit.providers.mongodb import MongoDbDaoFactory
from logot import Logot, logged

from tests_dcs.fixtures.joint import JointFixture

CHANGE_EVENT_TYPE = "upserted"
DELETE_EVENT_TYPE = "deleted"

TEST_FILE_ID = "test_id"

TEST_FILE_DELETION_REQUESTED = event_schemas.FileDeletionRequested(file_id=TEST_FILE_ID)


def test_make_record_from_update():
    """Test the get_record function"""
    record = make_record_from_update(TEST_FILE_DELETION_REQUESTED)
    assert record.model_dump() == {
        "correlation_id": get_correlation_id(),
        "file_id": TEST_FILE_ID,
    }


@pytest.mark.asyncio()
async def test_idempotence(joint_fixture: JointFixture, logot: Logot):
    """Test the idempotence functionality when encountering a record that already exists"""
    # First, insert the record that we want to collide with
    dao_factory = MongoDbDaoFactory(config=joint_fixture.config)
    dao = await get_file_deletion_requested_dao(dao_factory=dao_factory)
    record = models.FileDeletionRequestedRecord(
        correlation_id=get_correlation_id(), file_id=TEST_FILE_ID
    )

    record_is_new = await assert_record_is_new(
        dao=dao,
        resource_id=TEST_FILE_ID,
        update=TEST_FILE_DELETION_REQUESTED,
        record=record,
    )

    assert record_is_new

    # insert record
    await dao.insert(record)

    # check again, this time the result should be False and a debug log should be emitted
    record_is_new = await assert_record_is_new(
        dao=dao,
        resource_id=TEST_FILE_ID,
        update=TEST_FILE_DELETION_REQUESTED,
        record=record,
    )

    assert not record_is_new

    # examine logs
    logot.assert_logged(
        logged.debug(
            "Event with 'FileDeletionRequested' schema for resource ID 'test_id' has"
            + " already been processed under current correlation_id. Skipping."
        )
    )


@pytest.mark.asyncio()
async def test_outbox_subscriber_routing(joint_fixture: JointFixture):
    """Make sure the correct core methods are called from the outbox subscriber for
    each event type.
    """
    await joint_fixture.kafka.publish_event(
        payload=TEST_FILE_DELETION_REQUESTED.model_dump(),
        type_=CHANGE_EVENT_TYPE,
        topic=joint_fixture.config.files_to_delete_topic,
        key=TEST_FILE_ID,
    )

    mock = AsyncMock()
    joint_fixture.idempotence_handler.upsert_file_deletion_requested = mock

    await joint_fixture.outbox_subscriber.run(forever=False)
    mock.assert_awaited_once()


@pytest.mark.asyncio()
async def test_deletion_logs(joint_fixture: JointFixture, logot: Logot):
    """Test that the outbox subscriber logs deletions correctly.
    Consume a 'DELETED' event type for the outbox event.
    """
    # publish test event
    await joint_fixture.kafka.publish_event(
        payload=TEST_FILE_DELETION_REQUESTED.model_dump(),
        type_=DELETE_EVENT_TYPE,
        topic=joint_fixture.config.files_to_delete_topic,
        key=TEST_FILE_ID,
    )
    # consume that event
    await joint_fixture.outbox_subscriber.run(forever=False)

    # verify the log
    logot.assert_logged(
        logged.warning(
            f"Received DELETED-type event for FileDeletionRequested with resource ID {TEST_FILE_ID}",
        )
    )


@pytest.mark.asyncio()
async def test_file_registry_idempotence(
    joint_fixture: JointFixture,
    logot: Logot,
):
    """Test that the idempotence handler processes events correctly.

    This tests the method inside the data repository that is called by the outbox.
    The method is patched with a mock that can be inspected later for calls.
    The expected behavior is that the method is called once, and then not called again,
    because the record is already in the database.
    """
    mock = AsyncMock()

    joint_fixture.data_repository.delete_file = mock

    # Set which 'upsert_xyz' method to call on the idempotence handler
    method_to_call = joint_fixture.idempotence_handler.upsert_file_deletion_requested

    # call idempotence handler method once, which should call the file registry method
    await method_to_call(resource_id=TEST_FILE_ID, update=TEST_FILE_DELETION_REQUESTED)

    mock.assert_awaited_once()
    mock.reset_mock()

    # call the method once more, which should emit a debug log and not hit the registry
    await method_to_call(resource_id=TEST_FILE_ID, update=TEST_FILE_DELETION_REQUESTED)

    mock.assert_not_awaited()

    logot.assert_logged(
        logged.debug(
            f"Event with 'FileDeletionRequested' schema for resource ID '{TEST_FILE_ID}'"
            + " has already been processed under current correlation_id. Skipping."
        )
    )
