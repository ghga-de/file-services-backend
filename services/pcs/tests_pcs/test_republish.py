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
from ghga_event_schemas.pydantic_ import FileDeletionRequested
from hexkit.correlation import get_correlation_id, set_new_correlation_id
from hexkit.protocols.daosub import DaoSubscriberProtocol
from hexkit.providers.akafka import KafkaOutboxSubscriber
from hexkit.providers.akafka.testutils import ExpectedEvent

from pcs.config import Config
from tests_pcs.fixtures.joint import JointFixture


class DummySubTranslator(DaoSubscriberProtocol):
    """A class that consumes FileDeletionRequested events."""

    event_topic: str = ""
    dto_model = FileDeletionRequested
    correlation_ids: list[str]
    consumed_events: list[str]

    def __init__(self, *, config: Config) -> None:
        self.event_topic = config.files_to_delete_topic
        self.correlation_ids = []
        self.consumed_events = []

    async def changed(self, resource_id: str, update: FileDeletionRequested) -> None:
        """Consume event and record correlation ID"""
        self.correlation_ids.append(get_correlation_id())
        self.consumed_events.append(resource_id)

    async def deleted(self, resource_id: str) -> None:
        """Dummy"""


@pytest.mark.asyncio()
async def test_republish(joint_fixture: JointFixture):
    """Ensure the republish command on the DAO will work.

    Check that the event is republished with the correct correlation ID.
    """
    correlation_ids: list[str] = []
    file_ids: list[str] = ["test_id1", "test_id2"]

    for file_id in file_ids:
        file_deletion = FileDeletionRequested(file_id=file_id)
        payload = file_deletion.model_dump()
        event = ExpectedEvent(payload=payload, type_="upserted", key=file_id)

        # Publish one event at a time and verify
        async with set_new_correlation_id():
            correlation_ids.append(get_correlation_id())
            async with joint_fixture.kafka.expect_events(
                events=[event], in_topic=joint_fixture.config.files_to_delete_topic
            ):
                await joint_fixture.dao.insert(file_deletion)

    # Republish under new correlation ID to ensure it doesn't pollute the action
    async with set_new_correlation_id():
        await joint_fixture.dao.republish()

        # consume the republished events with the dummy translator
        translator = DummySubTranslator(config=joint_fixture.config)
        async with KafkaOutboxSubscriber.construct(
            config=joint_fixture.config, translators=[translator]
        ) as subscriber:
            await subscriber.run(forever=False)
            await subscriber.run(forever=False)

        # verify that the correlation IDs match what we expect. Sort first
        translator.correlation_ids.sort()
        translator.consumed_events.sort()
        correlation_ids.sort()
        assert translator.correlation_ids == correlation_ids
        assert translator.consumed_events == file_ids
