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
from ghga_event_schemas import pydantic_ as event_schemas
from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.correlation import get_correlation_id
from hexkit.custom_types import JsonObject
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb import MongoDbDaoFactory
from ifrs.adapters.outbound.dao import get_outbox_dao_collection
from ifrs.core import models
from ifrs.core.utils import assert_record_is_new, make_record_from_update
from ifrs.inject import prepare_outbox_subscriber
from logot import Logot, logged
from pydantic import BaseModel

from tests_ifrs.fixtures.config import get_config
from tests_ifrs.fixtures.dummy_file_registry import DummyFileRegistry
from tests_ifrs.fixtures.joint import JointFixture

CHANGE_EVENT_TYPE = "upserted"
DELETE_EVENT_TYPE = "deleted"

pytestmark = pytest.mark.asyncio()

TEST_FILE_ID = "test_id"

TEST_NONSTAGED_FILE_REQUESTED = event_schemas.NonStagedFileRequested(
    file_id=TEST_FILE_ID,
    target_object_id="",
    target_bucket_id="",
    s3_endpoint_alias="",
    decrypted_sha256="",
)

TEST_FILE_UPLOAD_VALIDATION_SUCCESS = event_schemas.FileUploadValidationSuccess(
    upload_date=now_as_utc().isoformat(),
    file_id=TEST_FILE_ID,
    object_id="",
    bucket_id="",
    s3_endpoint_alias="",
    decrypted_size=0,
    decryption_secret_id="",
    content_offset=0,
    encrypted_part_size=0,
    encrypted_parts_md5=[],
    encrypted_parts_sha256=[],
    decrypted_sha256="",
)

TEST_FILE_DELETION_REQUESTED = event_schemas.FileDeletionRequested(file_id=TEST_FILE_ID)


def test_make_record_from_update():
    """Test the get_record function"""
    record = make_record_from_update(
        models.FileDeletionRequestedRecord,
        event_schemas.FileDeletionRequested(file_id=TEST_FILE_ID),
    )
    assert record.model_dump() == {
        "correlation_id": get_correlation_id(),
        "file_id": TEST_FILE_ID,
    }


@pytest.mark.parametrize("prepopulate", [True, False])
async def test_idempotence_function(
    joint_fixture: JointFixture, logot: Logot, prepopulate: bool
):
    """Test the idempotence functionality when encountering a record that already exists"""
    # First, insert the record that we want to collide with
    dao_factory = MongoDbDaoFactory(config=joint_fixture.config)
    outbox_collection = await get_outbox_dao_collection(dao_factory=dao_factory)
    dao = outbox_collection.get_file_deletion_requested_dao()
    record = models.FileDeletionRequestedRecord(
        correlation_id=get_correlation_id(), file_id=TEST_FILE_ID
    )

    # Conditional insert
    if prepopulate:
        await dao.insert(record)

    if await assert_record_is_new(
        dao=dao,
        resource_id=TEST_FILE_ID,
        update=TEST_FILE_DELETION_REQUESTED,
        record=record,
    ):
        assert not prepopulate  # If prepopulate is False, record should be new

    # examine logs
    if prepopulate:
        logot.assert_logged(
            logged.debug(
                "Event with 'FileDeletionRequested' schema for resource ID 'test_id' has"
                + " already been processed under current correlation_id. Skipping."
            )
        )


@pytest.mark.parametrize(
    "upsertion_event, topic_config_name, method_name",
    [
        (
            TEST_FILE_DELETION_REQUESTED.model_dump(),
            "files_to_delete_topic",
            "upsert_file_deletion_requested",
        ),
        (
            TEST_FILE_UPLOAD_VALIDATION_SUCCESS.model_dump(),
            "files_to_register_topic",
            "upsert_file_upload_validation_success",
        ),
        (
            TEST_NONSTAGED_FILE_REQUESTED.model_dump(),
            "files_to_stage_topic",
            "upsert_nonstaged_file_requested",
        ),
    ],
)
async def test_outbox_subscriber_routing(
    kafka: KafkaFixture,
    upsertion_event: JsonObject,
    topic_config_name: str,
    method_name: str,
):
    """Make sure the correct core methods are called from the outbox subscriber for
    each event type.
    """
    config = get_config([kafka.config])
    dummy_file_registry = DummyFileRegistry()
    topic = getattr(config, topic_config_name)

    await kafka.publish_event(
        payload=upsertion_event,
        type_=CHANGE_EVENT_TYPE,
        topic=topic,
        key=TEST_FILE_ID,
    )

    async with prepare_outbox_subscriber(
        config=config, core_override=dummy_file_registry
    ) as outbox_subscriber:
        await outbox_subscriber.run(forever=False)
    assert dummy_file_registry.last_call == method_name


@pytest.mark.parametrize(
    "deletion_event, topic_config_name, event_type",
    [
        (
            TEST_FILE_DELETION_REQUESTED.model_dump(),
            "files_to_delete_topic",
            "FileDeletionRequested",
        ),
        (
            TEST_FILE_UPLOAD_VALIDATION_SUCCESS.model_dump(),
            "files_to_register_topic",
            "FileUploadValidationSuccess",
        ),
        (
            TEST_NONSTAGED_FILE_REQUESTED.model_dump(),
            "files_to_stage_topic",
            "NonStagedFileRequested",
        ),
    ],
)
async def test_deletion_logs(
    joint_fixture: JointFixture,
    logot: Logot,
    deletion_event: JsonObject,
    topic_config_name: str,
    event_type: str,
):
    """Test that the outbox subscriber logs deletions correctly.
    Consume a 'DELETED' event type for each of the outbox events.
    """
    topic = getattr(joint_fixture.config, topic_config_name)
    await joint_fixture.kafka.publish_event(
        payload=deletion_event,
        type_=DELETE_EVENT_TYPE,
        topic=topic,
        key=TEST_FILE_ID,
    )
    await joint_fixture.outbox_subscriber.run(forever=False)
    logot.assert_logged(
        logged.warning(
            f"Received DELETED-type event for {event_type} with resource ID '%s'",
        )
    )


@pytest.mark.parametrize(
    "update, method_to_patch, event_schema_name",
    [
        (
            TEST_FILE_DELETION_REQUESTED,
            "delete_file",
            "FileDeletionRequested",
        ),
        (
            TEST_FILE_UPLOAD_VALIDATION_SUCCESS,
            "register_file",
            "FileUploadValidationSuccess",
        ),
        (
            TEST_NONSTAGED_FILE_REQUESTED,
            "stage_registered_file",
            "NonStagedFileRequested",
        ),
    ],
)
async def test_file_registry_idempotence(
    joint_fixture: JointFixture,
    logot: Logot,
    update: BaseModel,
    method_to_patch: str,
    event_schema_name: str,
):
    """Test that the outbox-to-file registry interface handles events correctly.

    This tests the methods inside the file registry that are called by the outbox.
    The methods are patched with a mock that can be inspected later for calls.
    The expected behavior is that the method is called once, and then not called again,
    because the record is already in the database.
    """
    mock = AsyncMock()

    setattr(joint_fixture.file_registry, method_to_patch, mock)

    # Set which 'upsert_xyz' method to call on the file_registry
    method_to_call = joint_fixture.file_registry.upsert_nonstaged_file_requested
    if event_schema_name == "FileUploadValidationSuccess":
        method_to_call = (
            joint_fixture.file_registry.upsert_file_upload_validation_success
        )
    elif event_schema_name == "FileDeletionRequested":
        method_to_call = joint_fixture.file_registry.upsert_file_deletion_requested

    # call that method once, which should call the stub and insert the record
    await method_to_call(resource_id=TEST_FILE_ID, update=update)

    mock.assert_awaited_once()
    mock.reset_mock()

    # call the method once more, which should emit a debug log and not call the stub
    await method_to_call(resource_id=TEST_FILE_ID, update=update)

    mock.assert_not_awaited()

    logot.assert_logged(
        logged.debug(
            f"Event with '{event_schema_name}' schema for resource ID '{TEST_FILE_ID}'"
            + " has already been processed under current correlation_id. Skipping."
        )
    )
