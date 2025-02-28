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

"""Test to make sure that the DLQ is correctly set up for this service."""

import pytest
from ghga_event_schemas import pydantic_ as event_schemas

from tests_irs.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


async def test_event_subscriber_dlq(joint_fixture: JointFixture):
    """Verify that if we get an error when consuming an event, it gets published to the DLQ."""
    config = joint_fixture.config
    assert config.kafka_enable_dlq

    # Publish an event with a bogus payload to a topic/type this service expects
    await joint_fixture.kafka.publish_event(
        payload={"some_key": "some_value"},
        type_="upserted",
        topic=config.file_upload_received_topic,
        key="test",
    )
    async with joint_fixture.kafka.record_events(
        in_topic=config.kafka_dlq_topic
    ) as recorder:
        # Consume the event, which should error and get sent to the DLQ
        await joint_fixture.event_subscriber.run(forever=False)
    assert recorder.recorded_events
    assert len(recorder.recorded_events) == 1
    event = recorder.recorded_events[0]
    assert event.key == "test"
    assert event.payload == {"some_key": "some_value"}


async def test_consume_from_retry(joint_fixture: JointFixture):
    """Verify that this service will correctly get events from the retry topic.

    This involves publishing both outbox and non-outbox events to the retry
    topic to ensure they are consumed without issue.
    """
    config = joint_fixture.config
    assert config.kafka_enable_dlq

    outbox_payload = event_schemas.FileDeletionRequested(file_id="123")
    event_payload = event_schemas.FileInternallyRegistered(
        bucket_id="test",
        upload_date="2025-02-25T16:15:28.148287+00:00",
        file_id="",
        object_id="",
        s3_endpoint_alias="",
        decrypted_size=12345678,
        decrypted_sha256="fake-checksum",
        encrypted_size=123456789,
        encrypted_part_size=1,
        encrypted_parts_md5=["some", "checksum"],
        encrypted_parts_sha256=["some", "checksum"],
        content_offset=1234,
        decryption_secret_id="some-secret",
    )

    # Publish the outbox event
    await joint_fixture.kafka.publish_event(
        payload=outbox_payload.model_dump(),
        type_="upserted",
        topic=config.service_name + "-retry",
        key="test",
        headers={"original_topic": config.file_upload_received_topic},
    )

    # Publish the non-outbox event
    await joint_fixture.kafka.publish_event(
        payload=event_payload.model_dump(),
        type_=config.file_internally_registered_type,
        topic=config.service_name + "-retry",
        key="test",
        headers={"original_topic": config.file_internally_registered_topic},
    )

    # Consume the events (successful if it doesn't hang)
    await joint_fixture.event_subscriber.run(forever=False)
    await joint_fixture.event_subscriber.run(forever=False)
