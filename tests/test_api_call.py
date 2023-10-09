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
#

"""Test actual API call and event publishing"""

import pytest
from ghga_event_schemas.pydantic_ import FileUploadValidationSuccess
from hexkit.providers.akafka.testutils import (
    EventRecorder,
    ExpectedEvent,
    check_recorded_events,
)

from tests.fixtures.joint import (  # noqa: F401
    JointFixture,
    KafkaFixture,
    joint_fixture,
    kafka_fixture,
)


@pytest.mark.asyncio
async def test_api_call(monkeypatch, joint_fixture: JointFixture):  # noqa: F811
    """Test functionality with incoming API call"""
    event_recorder = EventRecorder(
        kafka_servers=joint_fixture.kafka.config.kafka_servers,
        topic=joint_fixture.config.publisher_topic,
    )

    secret_id = "very_secret_id"

    # test happy path
    headers = {"Authorization": f"Bearer {joint_fixture.token}"}

    # patch vault call with mock
    with monkeypatch.context() as patch:
        patch.setattr(
            "fis.adapters.outbound.vault.client.VaultAdapter.store_secret",
            lambda self, secret: secret_id,
        )
        async with event_recorder:
            response = await joint_fixture.rest_client.post(
                "/ingest", json=joint_fixture.encrypted_payload.dict(), headers=headers
            )

    assert response.status_code == 202
    assert len(event_recorder.recorded_events) == 1

    # can't get exact event time for equality comparison, don't check but get directly
    # from the recorded event instead
    expected_upload_date = str(event_recorder.recorded_events[0].payload["upload_date"])

    payload = FileUploadValidationSuccess(
        upload_date=expected_upload_date,
        file_id=joint_fixture.payload.file_id,
        object_id=joint_fixture.payload.object_id,
        bucket_id=joint_fixture.config.source_bucket_id,
        decrypted_size=joint_fixture.payload.unencrypted_size,
        decryption_secret_id=secret_id,
        content_offset=0,
        encrypted_part_size=joint_fixture.payload.part_size,
        encrypted_parts_md5=joint_fixture.payload.encrypted_md5_checksums,
        encrypted_parts_sha256=joint_fixture.payload.encrypted_sha256_checksums,
        decrypted_sha256=joint_fixture.payload.unencrypted_checksum,
    )

    expected_event = ExpectedEvent(
        payload=payload.dict(),
        type_=joint_fixture.config.publisher_type,
        key=joint_fixture.payload.file_id,
    )

    check_recorded_events(
        recorded_events=event_recorder.recorded_events, expected_events=[expected_event]
    )

    # test missing authorization
    response = await joint_fixture.rest_client.post(
        "/ingest", json=joint_fixture.encrypted_payload.dict()
    )
    assert response.status_code == 403

    # test malformed payload
    nonsense_payload = joint_fixture.encrypted_payload.copy(
        update={"payload": "abcdefghijklmn"}
    )
    response = await joint_fixture.rest_client.post(
        "/ingest", json=nonsense_payload.dict(), headers=headers
    )
    assert response.status_code == 422
