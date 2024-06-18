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

"""Testing for the republish functionality."""

import pytest
from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.correlation import get_correlation_id, set_new_correlation_id
from hexkit.protocols.daosub import DaoSubscriberProtocol
from hexkit.providers.akafka import KafkaOutboxSubscriber
from hexkit.providers.akafka.testutils import ExpectedEvent

from pcs.config import Config
from tests_pcs.fixtures.joint import JointFixture


class DummySubTranslator(DaoSubscriberProtocol):
    """A class that consumes FileDeletionRequested events."""

    event_topic: str = ""
    dto_model = event_schemas.FileDeletionRequested
    last_correlation_id: str

    def __init__(self, *, config: Config) -> None:
        self.event_topic = config.files_to_delete_topic

    async def changed(
        self, resource_id: str, update: event_schemas.FileDeletionRequested
    ) -> None:
        """Consume event and record correlation ID"""
        self.last_correlation_id = get_correlation_id()

    async def deleted(self, resource_id: str) -> None:
        """Dummy"""


@pytest.mark.asyncio()
async def test_republish(joint_fixture: JointFixture):
    """Ensure the republish command on the DAO will work.

    Check that the event is republished with the correct correlation ID.
    """
    event = event_schemas.FileDeletionRequested(file_id="test_id")
    payload = event.model_dump()
    expected_events = [ExpectedEvent(payload=payload, type_="upserted", key="test_id")]

    # Publish original message and verify
    async with set_new_correlation_id():
        correlation_id = get_correlation_id()
        async with joint_fixture.kafka.expect_events(
            events=expected_events, in_topic=joint_fixture.config.files_to_delete_topic
        ):
            await joint_fixture.dao.insert(event)

    # Republish and verify event content
    async with joint_fixture.kafka.expect_events(
        events=expected_events, in_topic=joint_fixture.config.files_to_delete_topic
    ):
        await joint_fixture.dao.republish()

    # Republish under new correlation ID and verify correlation ID matches old one
    async with set_new_correlation_id():
        new_correlation_id = get_correlation_id()
        assert new_correlation_id != correlation_id
        await joint_fixture.dao.republish()
        translator = DummySubTranslator(config=joint_fixture.config)
        async with KafkaOutboxSubscriber.construct(
            config=joint_fixture.config, translators=[translator]
        ) as subscriber:
            await subscriber.run(forever=False)
            assert translator.last_correlation_id == correlation_id
