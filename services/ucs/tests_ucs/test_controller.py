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

from asyncio import sleep
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.protocols.objstorage import ObjectStorageProtocol
from hexkit.utils import now_utc_ms_prec

from tests_ucs.fixtures import ConfigFixture
from tests_ucs.fixtures.dao_dummy import (
    DummyFileUploadBoxDao,
    DummyFileUploadDao,
    DummyS3UploadDetailsDao,
)
from ucs.core.controller import UploadController
from ucs.ports.inbound.controller import UploadControllerPort

pytestmark = pytest.mark.asyncio()

init_multipart_upload_mock = AsyncMock()
part_upload_url_mock = AsyncMock()
complete_upload_mock = AsyncMock()
abort_upload_mock = AsyncMock()


def id_gen(*args, **kwargs):
    """Return a stringified UUID4 while ignoring any parameters"""
    return str(uuid4())


@pytest.fixture(autouse=True)
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
        s3_upload_details_dao=AsyncMock(),
        object_storages=object_storages,
    )

    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    assert file_upload_box_dao.latest.id == box_id


async def test_create_new_file_upload(config: ConfigFixture):
    """Test creating a new FileUpload"""
    file_upload_box_dao = DummyFileUploadBoxDao()

    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
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


async def test_get_part_url(config: ConfigFixture):
    """Test getting a file part upload URL"""
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
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


async def test_complete_file_upload(config: ConfigFixture):
    """Test completing a multipart file upload"""
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
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
    assert file_upload_dao.latest.completed
    assert file_upload_box_dao.latest.size == 1024
    assert file_upload_box_dao.latest.file_count == 1

    # Now repeat the process to ensure the box stats are incremented, not overwritten
    await sleep(0.1)
    file_id2 = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file2", checksum="sha256:abc123", size=1000
    )
    await controller.complete_file_upload(box_id=box_id, file_id=file_id2)
    latest_s3_details = s3_upload_dao.latest
    assert latest_s3_details.file_id == file_id2
    assert latest_s3_details.completed
    assert latest_s3_details.completed > completed
    assert file_upload_box_dao.latest.file_count == 2
    assert file_upload_box_dao.latest.size == 2024


async def test_delete_file_upload(config: ConfigFixture):
    """Test deleting a FileUpload from a FileUploadBox"""
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
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
    file_upload_dao = DummyFileUploadDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=AsyncMock(),
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
    file_upload_dao = DummyFileUploadDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=AsyncMock(),
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
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # First create a FileUploadBox
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Create multiple FileUploads within the box
    file_id_1 = await controller.initiate_file_upload(
        box_id=box_id, alias="file_1", checksum="sha256:abc123", size=1024
    )
    file_id_2 = await controller.initiate_file_upload(
        box_id=box_id, alias="file_2", checksum="sha256:def456", size=2048
    )
    file_id_3 = await controller.initiate_file_upload(
        box_id=box_id, alias="file_3", checksum="sha256:ghi789", size=512
    )

    # Create a second box with different files to test isolation
    other_box_id = uuid4()
    await controller.create_file_upload_box(box_id=other_box_id, storage_alias="test")

    other_file_id_1 = await controller.initiate_file_upload(
        box_id=other_box_id, alias="other_file_1", checksum="sha256:xyz789", size=1536
    )
    other_file_id_2 = await controller.initiate_file_upload(
        box_id=other_box_id, alias="other_file_2", checksum="sha256:uvw456", size=3072
    )

    # Complete the file uploads so they appear in the results
    await controller.complete_file_upload(box_id=box_id, file_id=file_id_1)
    await controller.complete_file_upload(box_id=box_id, file_id=file_id_2)
    await controller.complete_file_upload(box_id=box_id, file_id=file_id_3)

    # Complete the uploads in the other box too
    await controller.complete_file_upload(box_id=other_box_id, file_id=other_file_id_1)
    await controller.complete_file_upload(box_id=other_box_id, file_id=other_file_id_2)

    # Create a third, empty box
    empty_box_id = uuid4()
    await controller.create_file_upload_box(box_id=empty_box_id, storage_alias="test")

    # Get the file IDs for the first box
    file_ids = await controller.get_file_ids_for_box(box_id=box_id)

    # Verify only the files from the first box are returned (not from other_box)
    expected_file_ids = {file_id_1, file_id_2, file_id_3}
    assert set(file_ids) == expected_file_ids

    # Also verify the other box returns its own files
    other_file_ids = await controller.get_file_ids_for_box(box_id=other_box_id)
    expected_other_file_ids = {other_file_id_1, other_file_id_2}
    assert set(other_file_ids) == expected_other_file_ids

    # Verify that we get an empty list for the third box
    no_ids = await controller.get_file_ids_for_box(box_id=empty_box_id)
    assert no_ids == []


async def test_create_box_duplicate(config: ConfigFixture):
    """Test for error handling when the user tries to create new FileUploadBox
    with an ID that already is used.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=AsyncMock(),
        s3_upload_details_dao=AsyncMock(),
        object_storages=object_storages,
    )

    # Create a FileUploadBox first
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Verify the box was created
    assert file_upload_box_dao.latest.id == box_id

    # Try to create another box with the same ID - should raise BoxAlreadyExistsError
    with pytest.raises(UploadControllerPort.BoxAlreadyExistsError) as exc_info:
        await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Verify the exception contains the correct box_id
    assert exc_info.value.box_id == box_id


async def test_create_box_with_unknown_storage_alias(config: ConfigFixture):
    """Test for error handling when the user tries to create new FileUploadBox
    with a storage alias that isn't configured.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=AsyncMock(),
        s3_upload_details_dao=AsyncMock(),
        object_storages=object_storages,
    )

    # Try to create a FileUploadBox with an unknown storage alias
    box_id = uuid4()
    unknown_storage_alias = "unknown_storage_alias_that_does_not_exist"

    # Should raise UnknownStorageAliasError
    with pytest.raises(UploadControllerPort.UnknownStorageAliasError) as exc_info:
        await controller.create_file_upload_box(
            box_id=box_id, storage_alias=unknown_storage_alias
        )

    # Verify the exception message contains the storage alias
    assert unknown_storage_alias in str(exc_info.value)

    # Verify no box was created in the DAO
    assert not file_upload_box_dao.resources


async def test_create_file_upload_alias_duplicate(config: ConfigFixture):
    """Test for error handling when a user tries to create a FileUpload
    for a file alias that already exists.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # First create a FileUploadBox
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Create a FileUpload with a specific alias
    file_alias = "duplicate_alias"
    await controller.initiate_file_upload(
        box_id=box_id, alias=file_alias, checksum="sha256:abc123", size=1024
    )

    # Try to create another FileUpload with the same alias - should raise FileUploadAlreadyExists
    with pytest.raises(UploadControllerPort.FileUploadAlreadyExists) as exc_info:
        await controller.initiate_file_upload(
            box_id=box_id, alias=file_alias, checksum="sha256:def456", size=2048
        )

    # Verify the exception message contains the alias
    assert file_alias in str(exc_info.value)

    # Verify only one FileUpload was created
    assert len(file_upload_dao.resources) == 1


async def test_create_file_upload_when_box_missing(config: ConfigFixture):
    """Test error handling in the case where the user tries to create a FileUpload
    for a box ID that doesn't exist.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Try to create a FileUpload for a non-existent box
    non_existent_box_id = uuid4()

    # Should raise BoxNotFoundError
    with pytest.raises(UploadControllerPort.BoxNotFoundError) as exc_info:
        await controller.initiate_file_upload(
            box_id=non_existent_box_id,
            alias="test_file",
            checksum="sha256:abc123",
            size=1024,
        )

    # Verify the exception contains the correct box_id
    assert str(non_existent_box_id) in str(exc_info.value)

    # Verify no FileUpload was created
    assert not file_upload_dao.resources
    assert not s3_upload_dao.resources


async def test_create_file_upload_when_box_locked(config: ConfigFixture):
    """Test error handling in the case where the user tries to create a FileUpload
    in a locked FileUploadBox.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # First create a FileUploadBox and lock it
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    await controller.lock_file_upload_box(box_id=box_id)
    assert file_upload_box_dao.latest.locked

    # Try to create a FileUpload in the locked box - should raise LockedBoxError
    with pytest.raises(UploadControllerPort.LockedBoxError) as exc_info:
        await controller.initiate_file_upload(
            box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
        )

    # Verify the exception contains the correct box_id
    assert str(box_id) in str(exc_info.value)

    # Verify no FileUpload was created
    assert not file_upload_dao.resources
    assert not s3_upload_dao.resources


async def test_delete_file_upload_when_box_missing(config: ConfigFixture):
    """Test error handling in the case where the user tries to delete a FileUpload
    for a box ID that doesn't exist.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Try to delete a FileUpload from a non-existent box
    non_existent_box_id = uuid4()
    fake_file_id = uuid4()

    # Should raise BoxNotFoundError
    with pytest.raises(UploadControllerPort.BoxNotFoundError) as exc_info:
        await controller.remove_file_upload(
            box_id=non_existent_box_id, file_id=fake_file_id
        )

    # Verify the exception contains the correct box_id
    assert str(non_existent_box_id) in str(exc_info.value)

    # Verify no changes were made to DAOs
    assert not file_upload_box_dao.resources
    assert not file_upload_dao.resources
    assert not s3_upload_dao.resources


async def test_delete_file_upload_when_box_locked(config: ConfigFixture):
    """Test error handling in the case where the user tries to delete a FileUpload
    in a locked FileUploadBox.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # First create a FileUploadBox and FileUpload
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Lock the box manually
    box = await file_upload_box_dao.get_by_id(box_id)
    box.locked = True
    await file_upload_box_dao.update(box)
    assert file_upload_box_dao.latest.locked

    # Try to delete the FileUpload from the locked box - should raise LockedBoxError
    with pytest.raises(UploadControllerPort.LockedBoxError) as exc_info:
        await controller.remove_file_upload(box_id=box_id, file_id=file_id)

    # Verify the exception contains the correct box_id
    assert str(box_id) in str(exc_info.value)

    # Verify the FileUpload and S3UploadDetails still exist (weren't deleted)
    assert len(file_upload_dao.resources) == 1
    assert len(s3_upload_dao.resources) == 1


async def test_delete_file_upload_when_upload_missing(config: ConfigFixture):
    """Test error handling in the case where the user tries to delete a FileUpload
    that doesn't exist.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Try to delete a FileUpload that doesn't exist - this should NOT raise an error
    await controller.remove_file_upload(box_id=box_id, file_id=uuid4())


async def test_delete_file_upload_with_missing_s3_details(config: ConfigFixture):
    """Test error handling in the case where the user tries to delete a FileUpload
    where the s3 upload details are missing. This would be an unusual case,
    but we're still testing it.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Verify both FileUpload and S3UploadDetails were created
    assert len(file_upload_dao.resources) == 1
    assert len(s3_upload_dao.resources) == 1

    # Manually delete the S3UploadDetails but leave the FileUpload
    # This simulates a data inconsistency where S3 details are missing
    await s3_upload_dao.delete(file_id)

    # Verify the FileUpload exists but S3UploadDetails are gone
    assert len(file_upload_dao.resources) == 1
    assert len(s3_upload_dao.resources) == 0

    # Try to delete the file upload - should raise S3UploadDetailsNotFoundError
    with pytest.raises(UploadControllerPort.S3UploadDetailsNotFoundError) as exc_info:
        await controller.remove_file_upload(box_id=box_id, file_id=file_id)

    # Verify the exception contains the correct file_id
    assert str(file_id) in str(exc_info.value)

    # Verify the FileUpload still exists (deletion was aborted due to missing S3 details)
    assert len(file_upload_dao.resources) == 1


async def test_delete_file_upload_with_s3_error(config: ConfigFixture):
    """Test for error handling when the user tries to delete a FileUpload
    but gets an S3 error in the process of aborting an ongoing upload.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Configure the abort mock to raise a MultiPartUploadAbortError
    # This simulates S3 failing to abort the multipart upload
    abort_upload_mock.side_effect = ObjectStorageProtocol.MultiPartUploadAbortError(
        upload_id="test_upload_id", bucket_id="test_bucket", object_id=str(file_id)
    )

    # Try to delete the file upload - should raise UploadAbortError
    with pytest.raises(UploadControllerPort.UploadAbortError) as exc_info:
        await controller.remove_file_upload(box_id=box_id, file_id=file_id)

    # Verify the exception contains the S3 upload ID
    s3_upload_id = s3_upload_dao.latest.s3_upload_id
    assert s3_upload_id in str(exc_info.value)

    # Verify the FileUpload and S3UploadDetails still exist (deletion failed)
    assert len(file_upload_dao.resources) == 1
    assert len(s3_upload_dao.resources) == 1


async def test_unlock_missing_box(config: ConfigFixture):
    """Test error handling for case where the user tries to unlock a missing box."""
    file_upload_box_dao = DummyFileUploadBoxDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=AsyncMock(),
        s3_upload_details_dao=AsyncMock(),
        object_storages=object_storages,
    )

    # Try to unlock a non-existent box
    non_existent_box_id = uuid4()
    with pytest.raises(UploadControllerPort.BoxNotFoundError) as exc_info:
        await controller.unlock_file_upload_box(box_id=non_existent_box_id)

    # Verify the exception contains the correct box_id
    assert str(non_existent_box_id) in str(exc_info.value)


async def test_lock_missing_box(config: ConfigFixture):
    """Test error handling for case where the user tries to lock a missing box."""
    file_upload_box_dao = DummyFileUploadBoxDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=AsyncMock(),
        s3_upload_details_dao=AsyncMock(),
        object_storages=object_storages,
    )

    # Try to lock a non-existent box
    non_existent_box_id = uuid4()

    # Should raise BoxNotFoundError
    with pytest.raises(UploadControllerPort.BoxNotFoundError) as exc_info:
        await controller.lock_file_upload_box(box_id=non_existent_box_id)

    # Verify the exception contains the correct box_id
    assert str(non_existent_box_id) in str(exc_info.value)


async def test_lock_box_with_incomplete_upload(config: ConfigFixture):
    """Test error handling for the scenario where the user tries to lock a box
    for which incomplete FileUpload(s) exist.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    file_id1 = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )
    file_id2 = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file2", checksum="sha256:abc123", size=1024
    )
    file_ids = sorted([file_id1, file_id2])

    # Attempt to lock the box while the upload is still incomplete
    with pytest.raises(UploadControllerPort.IncompleteUploadsError) as exc_info:
        await controller.lock_file_upload_box(box_id=box_id)

    # Verify the exception is correct
    assert (
        str(exc_info.value)
        == f"Cannot lock box {box_id} because these files are incomplete: {file_ids}"
    )

    # Verify that the box is still unlocked
    assert not file_upload_box_dao.latest.locked


async def test_complete_file_upload_when_box_missing(config: ConfigFixture):
    """Test error handling in the case where the user tries to complete a FileUpload
    for a box ID that doesn't exist.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Manually delete the box (simulating a scenario where the box was deleted
    # but the file upload and S3 details remain orphaned)
    await file_upload_box_dao.delete(box_id)

    # Verify the box is gone but file upload and S3 details remain
    assert not file_upload_box_dao.resources
    assert len(file_upload_dao.resources) == 1
    assert len(s3_upload_dao.resources) == 1

    # Try to complete the file upload for the now missing box
    with pytest.raises(UploadControllerPort.BoxNotFoundError) as exc_info:
        await controller.complete_file_upload(box_id=box_id, file_id=file_id)

    # Verify the exception contains the correct box_id
    assert str(box_id) in str(exc_info.value)
    assert not file_upload_dao.latest.completed
    assert not s3_upload_dao.latest.completed


async def test_complete_missing_file_upload(config: ConfigFixture):
    """Test error handling in the case where the user tries to complete a FileUpload
    that doesn't exist.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a box first
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")

    # Try to complete a file upload that doesn't exist
    non_existent_file_id = uuid4()

    with pytest.raises(UploadControllerPort.FileUploadNotFound) as exc_info:
        await controller.complete_file_upload(
            box_id=box_id, file_id=non_existent_file_id
        )
    assert not file_upload_dao.resources
    assert not s3_upload_dao.resources

    # Verify the exception contains the correct file_id
    assert str(non_existent_file_id) in str(exc_info.value)


async def test_complete_file_upload_with_missing_s3_details(config: ConfigFixture):
    """Test error handling in the case where the user tries to complete a FileUpload
    where the s3 upload details are missing. This would be an unusual case,
    but we're still testing it.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Manually delete the S3UploadDetails but leave the FileUpload
    # This simulates a data inconsistency where S3 details are missing
    await s3_upload_dao.delete(file_id)

    # Try to complete the file upload - should raise S3UploadDetailsNotFoundError
    with pytest.raises(UploadControllerPort.S3UploadDetailsNotFoundError) as exc_info:
        await controller.complete_file_upload(box_id=box_id, file_id=file_id)

    # Verify the exception contains the correct file_id
    assert str(file_id) in str(exc_info.value)

    # Verify the FileUpload is still marked as incomplete
    assert not file_upload_dao.latest.completed
    assert file_upload_box_dao.latest.size == 0
    assert file_upload_box_dao.latest.file_count == 0


async def test_complete_file_upload_with_unknown_storage_alias(config: ConfigFixture):
    """Test for error handling when the user tries to complete a FileUpload
    with a storage alias that isn't configured.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload with a valid storage alias
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Verify both FileUpload and S3UploadDetails were created
    assert len(file_upload_dao.resources) == 1
    assert len(s3_upload_dao.resources) == 1

    # Manually modify the S3UploadDetails to have an unknown storage alias
    # This simulates a scenario where configuration changed or data was corrupted
    s3_details = s3_upload_dao.latest
    s3_details.storage_alias = "does_not_exist"
    await s3_upload_dao.update(s3_details)

    # Try to complete the file upload - should raise UnknownStorageAliasError
    with pytest.raises(UploadControllerPort.UnknownStorageAliasError) as exc_info:
        await controller.complete_file_upload(box_id=box_id, file_id=file_id)

    # Verify the exception message contains the unknown storage alias
    assert "does_not_exist" in str(exc_info.value)
    assert not file_upload_dao.latest.completed
    assert file_upload_box_dao.latest.size == 0
    assert file_upload_box_dao.latest.file_count == 0


async def test_complete_file_upload_with_s3_error(config: ConfigFixture):
    """Test for error handling when the user tries to complete a FileUpload
    but gets an S3 error in the process.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Configure the complete mock to raise a MultiPartUploadConfirmError
    # This simulates S3 failing to complete the multipart upload
    complete_upload_mock.side_effect = (
        ObjectStorageProtocol.MultiPartUploadConfirmError(
            upload_id="test_upload_id", bucket_id="test_bucket", object_id=str(file_id)
        )
    )

    # Try to complete the file upload - should raise UploadCompletionError
    with pytest.raises(UploadControllerPort.UploadCompletionError) as exc_info:
        await controller.complete_file_upload(box_id=box_id, file_id=file_id)

    # Verify the exception contains the S3 upload ID
    s3_upload_id = s3_upload_dao.latest.s3_upload_id
    assert s3_upload_id in str(exc_info.value)
    assert not file_upload_dao.latest.completed
    assert not s3_upload_dao.latest.completed
    assert file_upload_box_dao.latest.size == 0
    assert file_upload_box_dao.latest.file_count == 0


async def test_get_part_upload_url_with_missing_file_id(config: ConfigFixture):
    """Test for error handling when getting a part URL but there's no S3UploadDetails
    document with a matching file_id.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Manually delete the S3UploadDetails but leave the FileUpload
    await s3_upload_dao.delete(file_id)

    # Try to get a part upload URL - should raise S3UploadDetailsNotFoundError
    with pytest.raises(UploadControllerPort.S3UploadDetailsNotFoundError) as exc_info:
        await controller.get_part_upload_url(file_id=file_id, part_no=1)

    # Verify the exception contains the correct file_id
    assert str(file_id) in str(exc_info.value)


async def test_get_part_upload_url_with_unknown_storage_alias(config: ConfigFixture):
    """Test for error handling when getting a part URL but the storage alias found in
    the relevant S3UploadDetails document is unknown (maybe configuration changed or
    data was migrated improperly).
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload with a valid storage alias
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Manually modify the S3UploadDetails to have an unknown storage alias
    # This simulates a scenario where configuration changed or data was corrupted
    s3_details = s3_upload_dao.latest
    s3_details.storage_alias = "unknown_storage_alias"
    await s3_upload_dao.update(s3_details)

    # Try to get a part upload URL - should raise UnknownStorageAliasError
    with pytest.raises(UploadControllerPort.UnknownStorageAliasError) as exc_info:
        await controller.get_part_upload_url(file_id=file_id, part_no=1)

    # Verify the exception message contains the unknown storage alias
    assert "unknown_storage_alias" in str(exc_info.value)


async def test_get_part_upload_url_when_s3_upload_not_found(config: ConfigFixture):
    """Test for error handling when getting a part URL but S3 raises an error saying
    that it can't find the corresponding multipart upload on its end.
    """
    file_upload_box_dao = DummyFileUploadBoxDao()
    file_upload_dao = DummyFileUploadDao()
    s3_upload_dao = DummyS3UploadDetailsDao()
    object_storages = S3ObjectStorages(config=config.config)

    controller = UploadController(
        config=config.config,
        file_upload_box_dao=file_upload_box_dao,
        file_upload_dao=file_upload_dao,
        s3_upload_details_dao=s3_upload_dao,
        object_storages=object_storages,
    )

    # Create a FileUploadBox and a FileUpload
    box_id = uuid4()
    await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
    file_id = await controller.initiate_file_upload(
        box_id=box_id, alias="test_file", checksum="sha256:abc123", size=1024
    )

    # Configure the part_upload_url_mock to raise a MultiPartUploadNotFoundError
    # This simulates S3 not being able to find the multipart upload
    part_upload_url_mock.side_effect = (
        ObjectStorageProtocol.MultiPartUploadNotFoundError(
            upload_id=s3_upload_dao.latest.s3_upload_id,
            bucket_id="test_bucket",
            object_id=str(file_id),
        )
    )

    # Try to get a part upload URL - should raise S3UploadNotFoundError
    with pytest.raises(UploadControllerPort.S3UploadNotFoundError) as exc_info:
        await controller.get_part_upload_url(file_id=file_id, part_no=1)

    # Verify the exception contains the S3 upload ID
    s3_upload_id = s3_upload_dao.latest.s3_upload_id
    assert s3_upload_id in str(exc_info.value)
