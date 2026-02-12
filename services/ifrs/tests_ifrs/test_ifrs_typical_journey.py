# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

import pytest
import requests
from hexkit.providers.akafka.testutils import ExpectedEvent
from hexkit.providers.s3.testutils import (
    FileObject,
    tmp_file,  # noqa: F401
)

from tests_ifrs.fixtures.example_data import EXAMPLE_ACCESSIONED_FILE
from tests_ifrs.fixtures.joint import JointFixture
from tests_ifrs.fixtures.utils import (
    DOWNLOAD_BUCKET,
    INTERROGATION_BUCKET,
    PERMANENT_BUCKET,
)

pytestmark = pytest.mark.asyncio


async def test_happy_journey(
    joint_fixture: JointFixture,
    tmp_file: FileObject,  # noqa: F811
):
    """Simulates a typical, successful journey for upload, download, and deletion"""
    storage_alias = joint_fixture.storage_aliases.node0
    storage = joint_fixture.federated_s3.storages[storage_alias]

    bucket_id = joint_fixture.config.object_storages[storage_alias].bucket
    # place example content in the Koeln interrogation bucket:
    file_object = tmp_file.model_copy(
        update={
            "bucket_id": INTERROGATION_BUCKET,
            "object_id": str(EXAMPLE_ACCESSIONED_FILE.id),
        }
    )

    # Populate test object, get the object size, and create the AccessionedFileUpload
    await storage.populate_file_objects(file_objects=[file_object])
    accessioned_file = EXAMPLE_ACCESSIONED_FILE.model_copy(
        update={
            "storage_alias": storage_alias,
            "encrypted_size": len(tmp_file.content),
        },
        deep=True,
    )
    assert accessioned_file.bucket_id == INTERROGATION_BUCKET  # just double checking

    # register new file from the interrogation bucket and make sure event is published
    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.file_internally_registered_topic,
    ) as recorder:
        await joint_fixture.file_registry.register_file(file=accessioned_file)

    stored_metadata = await joint_fixture.file_metadata_dao.get_by_id(
        EXAMPLE_ACCESSIONED_FILE.id
    )
    assert len(recorder.recorded_events) == 1
    event = recorder.recorded_events[0]
    assert event.type_ == joint_fixture.config.file_internally_registered_type
    # TODO: Test for key once it's decided if key should be accession or file ID
    assert stored_metadata.bucket_id != accessioned_file.bucket_id
    assert stored_metadata.bucket_id == PERMANENT_BUCKET
    stored_dumped = stored_metadata.model_dump(mode="json")
    event_payload = dict(event.payload)
    assert event_payload.pop("file_id") == stored_dumped.pop("id")

    # check that the file content is now in both the interrogation and permanent buckets
    object_id = str(EXAMPLE_ACCESSIONED_FILE.id)
    assert await storage.storage.does_object_exist(
        bucket_id=accessioned_file.bucket_id,
        object_id=object_id,
    )
    assert await storage.storage.does_object_exist(
        bucket_id=bucket_id,
        object_id=object_id,
    )

    # request a stage to the outbox:
    await joint_fixture.file_registry.stage_registered_file(
        accession=accessioned_file.accession,
        decrypted_sha256=accessioned_file.decrypted_sha256,
        download_object_id=EXAMPLE_ACCESSIONED_FILE.id,
        download_bucket_id=DOWNLOAD_BUCKET,
    )

    # check that the file content is now in all three storage entities:
    assert await storage.storage.does_object_exist(
        bucket_id=INTERROGATION_BUCKET,
        object_id=str(EXAMPLE_ACCESSIONED_FILE.id),
    )
    assert await storage.storage.does_object_exist(
        bucket_id=bucket_id,
        object_id=object_id,
    )
    assert await storage.storage.does_object_exist(
        bucket_id=DOWNLOAD_BUCKET,
        object_id=str(EXAMPLE_ACCESSIONED_FILE.id),
    )

    # check that the file content in the download bucket is identical to the content in
    # the interrogation bucket.
    download_url = await storage.storage.get_object_download_url(
        bucket_id=DOWNLOAD_BUCKET,
        object_id=str(EXAMPLE_ACCESSIONED_FILE.id),
    )
    response = requests.get(download_url, timeout=60)
    response.raise_for_status()
    assert response.content == file_object.content

    # Request file deletion:
    async with joint_fixture.kafka.expect_events(
        events=[
            ExpectedEvent(
                payload={"file_id": accessioned_file.accession},
                type_=joint_fixture.config.file_deleted_type,
            )
        ],
        in_topic=joint_fixture.config.file_deleted_topic,
    ):
        await joint_fixture.file_registry.delete_file(
            accession=accessioned_file.accession
        )

    assert not await storage.storage.does_object_exist(
        bucket_id=bucket_id,
        object_id=object_id,
    )
