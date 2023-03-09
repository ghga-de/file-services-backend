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

import base64
import json

import pytest
import requests
from fastapi import status
from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.providers.akafka.testutils import ExpectedEvent
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject

from tests.fixtures.joint import *  # noqa: F403


@pytest.mark.asyncio
async def test_happy(
    populated_fixture: PopulatedFixture,  # noqa: F405,F811
    file_fixture: FileObject,  # noqa: F811
):
    """Simulates a typical, successful API journey."""
    drs_id = populated_fixture.drs_id
    example_file = populated_fixture.example_file
    joint_fixture = populated_fixture.joint_fixture

    # request access to the newly registered file:
    # (An check that an event is published indicating that the file is not in
    # outbox yet.)
    non_staged_requested_event = event_schemas.NonStagedFileRequested(
        file_id=example_file.file_id, decrypted_sha256=example_file.decrypted_sha256
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
            "object_id": example_file.file_id,
        }
    )
    await joint_fixture.s3.populate_file_objects([file_object])

    # retry the access request:
    # (An check that an event is published indicating that a download was served.)
    download_served_event = event_schemas.FileDownloadServed(
        file_id=example_file.file_id,
        decrypted_sha256=example_file.decrypted_sha256,
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
    presigned_url = drs_object_response.json()["access_methods"][0]["access_url"]["url"]
    dowloaded_file = requests.get(presigned_url, timeout=2)
    dowloaded_file.raise_for_status()
    assert dowloaded_file.content == file_object.content

    pubkey = base64.urlsafe_b64encode(b"valid_key").decode("utf-8")

    response = await joint_fixture.rest_client.get(
        f"/objects/invalid_id/envelopes/{pubkey}", timeout=60
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND

    response = await joint_fixture.rest_client.get(
        f"/objects/{drs_id}/envelopes/{pubkey}", timeout=60
    )
    assert response.status_code == status.HTTP_200_OK
