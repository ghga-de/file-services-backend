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

"""Tests for the UploadController class"""

from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.utils import now_utc_ms_prec

from tests_ucs.fixtures import ConfigFixture
from tests_ucs.fixtures.dao_dummy import (
    DummyFileUploadBoxDao,
    DummyFileUploadDao,
    DummyS3UploadDetailsDao,
)
from ucs.core.controller import UploadController

pytestmark = pytest.mark.asyncio()

init_multipart_upload_mock = AsyncMock()
part_upload_url_mock = AsyncMock()
complete_upload_mock = AsyncMock()
abort_upload_mock = AsyncMock()


def id_gen(*args, **kwargs):
    """Return a stringified UUID4 while ignoring any parameters"""
    return str(uuid4())


@pytest.fixture()
def patch_s3_calls(monkeypatch):
    """Patch the calls to S3

    What this will do:
    - init_multipart_upload: ignore input and return a uuid4 string
    - get_part_upload_url: return a static url string
    - complete_multipart_upload: ignores input, returns None
    - abort_multipart_upload: ignores input, returns None
    """
    init_multipart_upload_mock.reset_mock()
    init_multipart_upload_mock.side_effect = id_gen
    monkeypatch.setattr(
        "ucs.core.controller.ObjectStorageProtocol.init_multipart_upload",
        init_multipart_upload_mock,
    )

    part_upload_url_mock.reset_mock()
    part_upload_url_mock.return_value = (
        "https://s3.example.com/test-bucket/test-file?part=1&uploadId=123"
    )
    monkeypatch.setattr(
        "ucs.core.controller.ObjectStorageProtocol.get_part_upload_url",
        part_upload_url_mock,
    )

    complete_upload_mock.reset_mock()
    # complete_multipart_upload returns None on success
    complete_upload_mock.return_value = None
    monkeypatch.setattr(
        "ucs.core.controller.ObjectStorageProtocol.complete_multipart_upload",
        complete_upload_mock,
    )

    # abort_multipart_upload returns None on success
    abort_upload_mock.reset_mock()
    abort_upload_mock.return_value = None
    monkeypatch.setattr(
        "ucs.core.controller.ObjectStorageProtocol.abort_multipart_upload",
        abort_upload_mock,
    )


async def test_create_new_box(config: ConfigFixture):
    """Test creating a new FileUploadBox"""
    file_upload_box_dao = DummyFileUploadBoxDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=AsyncMock(),
        s3_upload_dao=AsyncMock(),
        object_storages=object_storages,
    )

    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    assert file_upload_box_dao.latest.id == box_id


async def test_create_new_file_upload(config: ConfigFixture, patch_s3_calls):
    """Test creating a new FileUpload"""
    file_upload_box_dao = DummyFileUploadBoxDao()

    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # First create a FileUploadBox
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Then create a FileUpload within the box
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Verify the FileUpload was created
    assert file_upload_dao.latest.id == file_id
    assert file_upload_dao.latest.alias == "test_file"
    assert file_upload_dao.latest.checksum == "sha256:abc123"
    assert file_upload_dao.latest.size == 1024

    # Verify S3UploadDetails were created
    upload_details = s3_upload_dao.latest
    assert upload_details.file_id == file_id
    assert upload_details.storage_alias == "test"
    assert now_utc_ms_prec() - upload_details.initiated < timedelta(seconds=5)
    assert not upload_details.completed
    assert not upload_details.deleted


async def test_get_part_url(config: ConfigFixture, patch_s3_calls):
    """Test getting a file part upload URL"""
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # First create a FileUploadBox
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Then create a FileUpload within the box
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Now get the part upload URL
    part_no = 1
    result_url = await controller.get_part_upload_url(file_id=file_id, part_no=part_no)

    # Verify the URL was returned
    assert (
        result_url == "https://s3.example.com/test-bucket/test-file?part=1&uploadId=123"
    )

    # Verify the S3 method was called
    part_upload_url_mock.assert_called_once()


async def test_complete_file_upload(config: ConfigFixture, patch_s3_calls):
    """Test completing a multipart file upload"""
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # First create a FileUploadBox
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Then create a FileUpload within the box
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Now complete the file upload
    await controller.complete_file_upload(box_id=box_id, file_id=file_id)

    # Verify the S3 method was called
    complete_upload_mock.assert_called_once()

    # Verify that the S3UploadDetails still exist (they should remain for tracking)
    completed = now_utc_ms_prec()
    assert s3_upload_dao.latest.file_id == file_id
    assert s3_upload_dao.latest.completed is not None
    assert completed - s3_upload_dao.latest.completed < timedelta(seconds=5)


async def test_delete_file_upload(config: ConfigFixture, patch_s3_calls):
    """Test deleting a FileUpload from a FileUploadBox"""
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # First create a FileUploadBox
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Then create a FileUpload within the box
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Verify the FileUpload exists before deletion
    assert len(file_upload_dao.resources) == 1
    assert len(s3_upload_dao.resources) == 1

    # Now delete the file upload
    await controller.remove_file_upload(box_id=box_id, file_id=file_id)

    # Verify the S3 abort method was called
    abort_upload_mock.assert_called_once()

    # Verify that the FileUpload and S3UploadDetails were removed
    assert not file_upload_dao.resources
    assert not s3_upload_dao.resources


async def test_lock_file_upload_box(config: ConfigFixture):
    """Test locking an unlocked FileUploadBox"""
    file_upload_box_dao = DummyFileUploadBoxDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=AsyncMock(),
        s3_upload_dao=AsyncMock(),
        object_storages=object_storages,
    )

    # First create a FileUploadBox (starts unlocked by default)
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Verify the box starts unlocked
    assert not file_upload_box_dao.latest.locked

    # Now lock the box
    await controller.lock_file_upload_box(box_id=box_id)

    # Verify the box is now locked
    assert file_upload_box_dao.latest.locked


async def test_unlock_file_upload_box(config: ConfigFixture):
    """Test unlocking a locked FileUploadBox"""
    file_upload_box_dao = DummyFileUploadBoxDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=AsyncMock(),
        s3_upload_dao=AsyncMock(),
        object_storages=object_storages,
    )

    # First create a FileUploadBox
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Lock the box first
    await controller.lock_file_upload_box(box_id=box_id)
    assert file_upload_box_dao.latest.locked

    # Now unlock the box
    await controller.unlock_file_upload_box(box_id=box_id)

    # Verify the box is now unlocked
    assert not file_upload_box_dao.latest.locked


async def test_get_box_uploads(config: ConfigFixture):
    """Test getting file IDs for a given box ID"""
