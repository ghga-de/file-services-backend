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
import re

import httpx
import pytest
from fastapi import status
from ghga_event_schemas import pydantic_ as event_schemas
from ghga_service_commons.api.mock_router import (  # noqa: F401
    assert_all_responses_were_requested,
)
from hexkit.providers.akafka.testutils import ExpectedEvent
from hexkit.providers.s3.testutils import FileObject
from pytest_httpx import HTTPXMock, httpx_mock  # noqa: F401

from tests.fixtures.joint import *  # noqa: F403
from tests.fixtures.mock_api.app import router

unintercepted_hosts: list[str] = ["localhost"]


@pytest.fixture
def non_mocked_hosts() -> list:
    """Fixture used by httpx_mock to determine which requests to intercept

    We only want to intercept calls to the EKSS API, so this list will include
    localhost and the host from the S3 fixture's connection URL.
    """
    return unintercepted_hosts


@pytest.mark.asyncio
async def test_happy_journey(
    populated_fixture: PopulatedFixture,  # noqa: F405,F811
    file_fixture: FileObject,  # noqa: F811
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """Simulates a typical, successful API journey."""
    drs_id = populated_fixture.drs_id
    example_file = populated_fixture.example_file
    joint_fixture = populated_fixture.joint_fixture
    object_id = populated_fixture.object_id

    # explicitly handle ekss API calls (and name unintercepted hosts above)
    httpx_mock.add_callback(
        callback=router.handle_request,
        url=re.compile(rf"^{joint_fixture.config.ekss_base_url}.*"),
    )

    # simplify testing by using one longer lived work order token

    # request access to the newly registered file:
    # (An check that an event is published indicating that the file is not in
    # outbox yet.)
    non_staged_requested_event = event_schemas.NonStagedFileRequested(
        file_id=example_file.file_id,
        target_object_id=object_id,
        target_bucket_id=joint_fixture.config.outbox_bucket,
        decrypted_sha256=example_file.decrypted_sha256,
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
        response = await joint_fixture.rest_client.get(f"/objects/{drs_id}", timeout=5)
    assert response.status_code == status.HTTP_202_ACCEPTED
    assert (
        int(response.headers["Retry-After"]) == joint_fixture.config.retry_access_after
    )

    # place the requested file into the outbox bucket (it is not important here that
    # the file content does not match the announced decrypted_sha256 checksum):
    file_object = file_fixture.copy(
        update={
            "bucket_id": joint_fixture.config.outbox_bucket,
            "object_id": object_id,
        }
    )

    await joint_fixture.s3.populate_file_objects([file_object])

    # retry the access request:
    # (An check that an event is published indicating that a download was served.)
    download_served_event = event_schemas.FileDownloadServed(
        file_id=example_file.file_id,
        target_object_id=object_id,
        target_bucket_id=joint_fixture.config.outbox_bucket,
        decrypted_sha256=example_file.decrypted_sha256,
        context="unknown",
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
    unintercepted_hosts.append(httpx.URL(presigned_url).host)
    dowloaded_file = httpx.get(presigned_url, timeout=5)
    dowloaded_file.raise_for_status()
    assert dowloaded_file.content == file_object.content

    response = await joint_fixture.rest_client.get(
        f"/objects/{drs_id}/envelopes", timeout=5
    )
    assert response.status_code == status.HTTP_200_OK

    response = await joint_fixture.rest_client.get(
        "/objects/invalid_id/envelopes", timeout=5
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    response = await joint_fixture.rest_client.get(
        f"/objects/{drs_id}/envelopes",
        timeout=5,
        headers={"Authorization": "Bearer invalid"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.asyncio
async def test_happy_deletion(
    populated_fixture: PopulatedFixture,  # noqa: F405,F811
    file_fixture: FileObject,  # noqa: F811
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """Simulates a typical, successful journey for file deletion."""
    drs_id = populated_fixture.drs_id
    joint_fixture = populated_fixture.joint_fixture
    object_id = populated_fixture.object_id

    # explicitly handle ekss API calls (and name unintercepted hosts above)
    httpx_mock.add_callback(
        callback=router.handle_request,
        url=re.compile(rf"^{joint_fixture.config.ekss_base_url}.*"),
    )

    # place example content in the outbox bucket:
    file_object = file_fixture.copy(
        update={
            "bucket_id": joint_fixture.config.outbox_bucket,
            "object_id": object_id,
        }
    )
    await joint_fixture.s3.populate_file_objects(file_objects=[file_object])

    data_repository = await joint_fixture.container.data_repository()

    # request a stage to the outbox:
    async with joint_fixture.kafka.expect_events(
        events=[
            ExpectedEvent(
                payload={
                    "file_id": drs_id,
                },
                type_=joint_fixture.config.file_deleted_event_type,
            )
        ],
        in_topic=joint_fixture.config.file_deleted_event_topic,
    ):
        await data_repository.delete_file(file_id=drs_id)

    assert not await joint_fixture.s3.storage.does_object_exist(
        bucket_id=joint_fixture.config.outbox_bucket,
        object_id=object_id,
    )


@pytest.mark.asyncio
async def test_cleanup(cleanup_fixture: CleanupFixture):  # noqa: F405,F811
    """Test outbox cleanup handling"""
    data_repository = await cleanup_fixture.joint_fixture.container.data_repository()
    await data_repository.cleanup_outbox()

    # check if object within threshold is still there
    cached_object = await cleanup_fixture.mongodb_dao.get_by_id(
        cleanup_fixture.cached_id
    )
    assert await cleanup_fixture.joint_fixture.s3.storage.does_object_exist(
        bucket_id=cleanup_fixture.joint_fixture.config.outbox_bucket,
        object_id=cached_object.object_id,
    )

    # check if expired object has been removed from outbox
    expired_object = await cleanup_fixture.mongodb_dao.get_by_id(
        cleanup_fixture.expired_id
    )
    assert not await cleanup_fixture.joint_fixture.s3.storage.does_object_exist(
        bucket_id=cleanup_fixture.joint_fixture.config.outbox_bucket,
        object_id=expired_object.object_id,
    )
