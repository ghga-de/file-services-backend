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

"""Tests typical user journeys"""

import json
from datetime import datetime

import pytest
import requests
from fastapi import status
from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.providers.akafka.testutils import ExpectedEvent
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject

from dcs.core import models
from tests.fixtures.joint import *  # noqa: F403

EXAMPLE_FILE = models.FileToRegister(
    file_id="examplefile001",
    decrypted_sha256="0677de3685577a06862f226bb1bfa8f889e96e59439d915543929fb4f011d096",
    creation_date=datetime.now().isoformat(),
    decrypted_size=12345,
)


@pytest.mark.asyncio
async def test_happy(
    joint_fixture: JointFixture,  # noqa: F811, F405
    file_fixture: FileObject,  # noqa: F811
):
    """Simulates a typical, successful API journey."""

    # publish an event to register a new file for download:
    files_to_register_event = event_schemas.FileInternallyRegistered(
        file_id=EXAMPLE_FILE.file_id,
        upload_date=EXAMPLE_FILE.creation_date,
        decrypted_size=EXAMPLE_FILE.decrypted_size,
        decrypted_sha256=EXAMPLE_FILE.decrypted_sha256,
        encrypted_part_size=1,
        encrypted_parts_md5=["some", "checksum"],
        encrypted_parts_sha256=["some", "checksum"],
        content_offset=1234,
        decryption_secret_id="some-secret",
    )
    await joint_fixture.kafka.publish_event(
        payload=json.loads(files_to_register_event.json()),
        type_=joint_fixture.config.files_to_register_type,
        topic=joint_fixture.config.files_to_register_topic,
    )

    # consume the event:
    event_subscriber = await joint_fixture.container.event_subscriber()
    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.file_registered_event_topic
    ) as recorder:
        await event_subscriber.run(forever=False)

    # check that an event informing about the newly registered file was published:
    assert len(recorder.recorded_events) == 1
    assert (
        recorder.recorded_events[0].type_
        == joint_fixture.config.file_registered_event_type
    )
    file_registered_event = event_schemas.FileRegisteredForDownload(
        **recorder.recorded_events[0].payload
    )
    assert file_registered_event.file_id == EXAMPLE_FILE.file_id
    assert file_registered_event.decrypted_sha256 == EXAMPLE_FILE.decrypted_sha256
    assert file_registered_event.upload_date == EXAMPLE_FILE.creation_date
    drs_id = file_registered_event.drs_uri.split("/")[-1]

    # request access to the newly registered file:
    # (An check that an event is published indicating that the file is not in
    # outbox yet.)
    non_staged_requested_event = event_schemas.NonStagedFileRequested(
        file_id=EXAMPLE_FILE.file_id, decrypted_sha256=EXAMPLE_FILE.decrypted_sha256
    )
    async with joint_fixture.kafka.expect_events(
        events=[
            ExpectedEvent(
                payload=json.loads(non_staged_requested_event.json()),
                type_=joint_fixture.config.unstaged_download_event_type,
            )
        ],
        in_topic=joint_fixture.config.unstaged_download_event_topic,
    ):
        response = await joint_fixture.rest_client.get(f"/objects/{drs_id}")
    assert response.status_code == status.HTTP_202_ACCEPTED
    assert (
        int(response.headers["Retry-After"]) == joint_fixture.config.retry_access_after
    )

    # place the requested file into the outbox bucket (it is not important here that
    # the file content does not match the announced decrypted_sha256 checksum):
    file_object = file_fixture.copy(
        update={
            "bucket_id": joint_fixture.config.outbox_bucket,
            "object_id": EXAMPLE_FILE.file_id,
        }
    )
    await joint_fixture.s3.populate_file_objects([file_object])

    # retry the access request:
    # (An check that an event is published indicating that a download was served.)
    download_served_event = event_schemas.FileDownloadServed(
        file_id=EXAMPLE_FILE.file_id,
        decrypted_sha256=EXAMPLE_FILE.decrypted_sha256,
        context="unkown",
    )
    async with joint_fixture.kafka.expect_events(
        events=[
            ExpectedEvent(
                payload=json.loads(download_served_event.json()),
                type_=joint_fixture.config.download_served_event_type,
            )
        ],
        in_topic=joint_fixture.config.download_served_event_topic,
    ):
        drs_object_response = await joint_fixture.rest_client.get(f"/objects/{drs_id}")

    # download file bytes:
    dowloaded_file = requests.get(drs_object_response.json()["access_url"], timeout=2)
    dowloaded_file.raise_for_status()
    assert dowloaded_file.content == file_object.content
