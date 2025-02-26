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
from hexkit.providers.akafka.testutils import KafkaFixture

from tests_ucs.fixtures.config import get_config
from tests_ucs.fixtures.joint import JointFixture
from ucs.inject import prepare_event_subscriber

pytestmark = pytest.mark.asyncio()


async def test_event_subscriber_dlq(kafka: KafkaFixture):
    """Verify that if we get an error when consuming an event, it gets published to the DLQ."""
    config = get_config(sources=[kafka.config], kafka_enable_dlq=True)
    assert config.kafka_enable_dlq

    # Publish an event with a bogus payload to a topic/type this service expects
    await kafka.publish_event(
        payload={"some_key": "some_value"},
        type_="upserted",
        topic=config.files_to_delete_topic,
        key="test",
    )
    async with kafka.record_events(in_topic=config.kafka_dlq_topic) as recorder:
        # Consume the event, which should error and get sent to the DLQ
        async with prepare_event_subscriber(config=config) as event_subscriber:
            await event_subscriber.run(forever=False)
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
    # Override the kafka test fixture's default for kafka_enable_dlq
    config = joint_fixture.config
    assert config.kafka_enable_dlq

    outbox_payload = event_schemas.FileDeletionRequested(file_id="123")
    event_payload = event_schemas.FileUploadValidationFailure(
        bucket_id="test",
        upload_date="2025-02-25T16:15:28.148287+00:00",
        file_id="",
        object_id="",
        s3_endpoint_alias="",
        reason="test",
    )

    # Publish an event with a proper payload to a topic/type this service expects
    await joint_fixture.kafka.publish_event(
        payload=outbox_payload.model_dump(),
        type_="upserted",
        topic=config.service_name + "-retry",
        key="test",
        headers={"original_topic": config.files_to_delete_topic},
    )
    await joint_fixture.kafka.publish_event(
        payload=event_payload.model_dump(),
        type_=config.upload_rejected_event_type,
        topic=config.service_name + "-retry",
        key="test",
        headers={"original_topic": config.upload_rejected_event_topic},
    )

    # Consume the events
    await joint_fixture.event_subscriber.run(forever=False)
    await joint_fixture.event_subscriber.run(forever=False)
