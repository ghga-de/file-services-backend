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

"""Implements the UploadController class to manage file uploads"""

import logging
from contextlib import suppress
from uuid import uuid4

from ghga_service_commons.utils.multinode_storage import ObjectStorages
from hexkit.protocols.objstorage import ObjectStorageProtocol
from hexkit.utils import now_utc_ms_prec
from pydantic import UUID4

from ucs.config import Config
from ucs.core.models import FileUpload, FileUploadBox, S3UploadDetails
from ucs.ports.inbound.controller import UploadControllerPort
from ucs.ports.outbound.dao import (
    FileUploadBoxDao,
    FileUploadDao,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    S3UploadDetailsDao,
)

log = logging.getLogger(__name__)


class UploadController(UploadControllerPort):
    """A class for managing file uploads"""

    def __init__(
        self,
        *,
        config: Config,
        file_upload_box_dao: FileUploadBoxDao,
        file_upload_dao: FileUploadDao,
        s3_upload_details_dao: S3UploadDetailsDao,
        object_storages: ObjectStorages,
    ):
        self._config = config
        self._file_upload_box_dao = file_upload_box_dao
        self._file_upload_dao = file_upload_dao
        self._s3_upload_details_dao = s3_upload_details_dao
        self._object_storages = object_storages

    def _get_bucket_and_storage(
        self, storage_alias: str
    ) -> tuple[str, ObjectStorageProtocol]:
        """Return the bucket ID and ObjectStorageProtocol for a given storage alias.

        Raises `UnknownStorageAliasError` if the storage alias is not known.
        """
        try:
            bucket_id, object_storage = self._object_storages.for_alias(storage_alias)
        except KeyError as error:
            unknown_alias = self.UnknownStorageAliasError(storage_alias=(storage_alias))
            log.error(unknown_alias, extra={"storage_alias": storage_alias})
            raise unknown_alias from error
        log.debug(
            "Found bucket '%s' and object storage for alias '%s'",
            bucket_id,
            storage_alias,
        )
        return bucket_id, object_storage

    async def _insert_validated_file_upload(
        self, *, box: FileUploadBox, alias: str, checksum: str, size: int
    ) -> UUID4:
        """Create a new FileUpload for the provided file alias and return the file_id.

        This method checks that a FileUpload doesn't already exist for the provided alias.
        If these conditions are met, then it inserts a new FileUpload with a random
        UUID4 for file_id.

        Raises `FileUploadAlreadyExists` if there's already a FileUpload for this alias.
        """
        box_id = box.id

        # Verify that a file hasn't been created for this box + alias already
        hits = [
            x
            async for x in self._file_upload_dao.find_all(
                mapping={"box_id": box_id, "alias": alias}
            )
        ]
        if hits:
            error = self.FileUploadAlreadyExists(alias=alias)
            log.error(
                error,
                extra={
                    "box_id": box.id,
                    "file_alias": alias,
                    "checksum": checksum,
                    "size": size,
                },
            )
            raise error

        # Insert the FileUpload object
        file_id = uuid4()
        file_upload = FileUpload(
            id=file_id, box_id=box_id, alias=alias, size=size, checksum=checksum
        )
        await self._file_upload_dao.insert(file_upload)

        return file_id

    async def _get_unlocked_box(self, *, box_id: UUID4) -> FileUploadBox:
        """Retrieve a FileUploadBox by ID.

        Raises:
        - `BoxNotFoundError` if the box does not exist
        - `LockedBoxError` if the box exists but is locked.
        """
        # Verify that the box exists
        try:
            box = await self._file_upload_box_dao.get_by_id(box_id)
        except ResourceNotFoundError as err:
            error = self.BoxNotFoundError(box_id=box_id)
            log.error(error)
            raise error from err

        # Verify that the box is not locked
        if box.locked:
            error = self.LockedBoxError(box_id=box_id)
            log.error(error)
            raise error

        return box

    async def _remove_completed_file_upload(
        self, *, s3_upload_details: S3UploadDetails
    ) -> None:
        """Delete a completely uploaded file from S3 and blindly try to delete the
        associated multipart upload just to be sure.

        Does not delete any data from the DB.

        Raises:
        - `UnknownStorageAliasError` if the storage alias is not known.
        """
        object_id = str(s3_upload_details.file_id)
        storage_alias = s3_upload_details.storage_alias
        bucket_id, object_storage = self._get_bucket_and_storage(
            storage_alias=storage_alias
        )

        if await object_storage.does_object_exist(
            bucket_id=bucket_id, object_id=object_id
        ):
            await object_storage.delete_object(bucket_id=bucket_id, object_id=object_id)

        # no way to check, just run and ignore exception
        with suppress(object_storage.MultiPartUploadNotFoundError):
            await object_storage.abort_multipart_upload(
                bucket_id=bucket_id,
                object_id=object_id,
                upload_id=s3_upload_details.s3_upload_id,
            )

    async def _remove_incomplete_file_upload(
        self, *, s3_upload_details: S3UploadDetails
    ) -> None:
        """Abort an incomplete S3 multipart upload.

        Does not delete any data from the DB.

        Raises:
        - `UnknownStorageAliasError` if the storage alias is not known.
        - `UploadAbortError` if there's an error instructing S3 to abort the upload.
        """
        file_id = s3_upload_details.file_id
        storage_alias = s3_upload_details.storage_alias
        s3_upload_id = s3_upload_details.s3_upload_id
        bucket_id, object_storage = self._get_bucket_and_storage(
            storage_alias=storage_alias
        )

        try:
            await object_storage.abort_multipart_upload(
                upload_id=s3_upload_id,
                bucket_id=bucket_id,
                object_id=str(file_id),
            )
        except object_storage.MultiPartUploadAbortError as err:
            error = self.UploadAbortError(
                file_id=file_id, s3_upload_id=s3_upload_id, bucket_id=bucket_id
            )
            log.error(
                error,
                exc_info=True,
                extra={
                    "file_id": file_id,
                    "bucket_id": bucket_id,
                    "storage_alias": storage_alias,
                    "s3_upload_id": s3_upload_id,
                },
            )
            raise error from err
        except object_storage.MultiPartUploadNotFoundError:
            # This correspond to an inconsistency between the database and
            # the storage, however, since this cancel method might be used to
            # resolve this inconsistency, this exception will be ignored.
            pass

    async def initiate_file_upload(
        self, *, box_id: UUID4, alias: str, checksum: str, size: int
    ) -> UUID4:
        """Initialize a new multipart upload and return the file ID.

        Raises:
        - `BoxNotFoundError` if the box does not exist.
        - `LockedBoxError` if the box exists but is locked.
        - `FileUploadAlreadyExists` if there's already a FileUpload for this alias.
        - `UnknownStorageAliasError` if the storage alias is not known.
        - `MultipartUploadDupeError` if an S3 upload is already in progress.
        """
        # Get the box and create the FileUpload
        box = await self._get_unlocked_box(box_id=box_id)
        file_id = await self._insert_validated_file_upload(
            box=box, alias=alias, checksum=checksum, size=size
        )
        log.info(
            "FileUpload %s added for alias %s.",
            file_id,
            alias,
            extra={"box_id": box_id},
        )

        # Get the S3 storage details
        storage_alias = box.storage_alias
        bucket_id, object_storage = self._get_bucket_and_storage(
            storage_alias=storage_alias
        )

        # Initiate a new multipart file upload on the S3 instance
        try:
            s3_upload_id = await object_storage.init_multipart_upload(
                bucket_id=bucket_id, object_id=str(file_id)
            )
            log.info(
                "S3 multipart upload %s created for file ID %s (file alias %s)",
                s3_upload_id,
                file_id,
                alias,
                extra={"storage_alias": storage_alias, "bucket_id": bucket_id},
            )
        except object_storage.MultiPartUploadAlreadyExistsError as err:
            error = self.MultipartUploadInProgressError(
                file_id=file_id, bucket_id=storage_alias
            )
            log.error(
                error,
                exc_info=True,
                extra={
                    "box_id": box_id,
                    "file_id": file_id,
                    "alias": alias,
                    "storage_alias": storage_alias,
                },
            )
            raise error from err

        # Insert S3UploadDetails. Don't check for duplicate because insert only
        #  occurs in this method and only if the FileUpload alias is new. The check for
        #  duplicates is performed in `_insert_validated_file_upload`.
        s3_upload = S3UploadDetails(
            file_id=file_id,
            storage_alias=box.storage_alias,
            s3_upload_id=s3_upload_id,
            initiated=now_utc_ms_prec(),
        )
        await self._s3_upload_details_dao.insert(s3_upload)
        log.debug(
            "S3UploadDetails created for file ID %s",
            file_id,
            extra={"storage_alias": storage_alias, "bucket_id": bucket_id},
        )

        return file_id

    async def get_part_upload_url(self, *, file_id: UUID4, part_no: int) -> str:
        """
        Create and return a pre-signed URL to upload the bytes for the file part with
        the given number of the upload with the given ID.

        Raises:
        - `S3UploadDetailsNotFoundError` if no upload details are found.
        - `UnknownStorageAliasError` if the storage alias is not known.
        - `S3UploadNotFoundError` if the S3 multipart upload can't be found.
        """
        # Retrieve the S3Upload record for this file ID
        try:
            s3_upload_details = await self._s3_upload_details_dao.get_by_id(file_id)
        except ResourceNotFoundError as err:
            error = self.S3UploadDetailsNotFoundError(file_id=file_id)
            log.error(
                error,
                extra={
                    "file_id": file_id,
                    "part_no": part_no,
                },
            )
            raise error from err
        storage_alias = s3_upload_details.storage_alias
        s3_upload_id = s3_upload_details.s3_upload_id

        bucket_id, object_storage = self._get_bucket_and_storage(
            storage_alias=storage_alias
        )

        try:
            return await object_storage.get_part_upload_url(
                upload_id=s3_upload_id,
                bucket_id=bucket_id,
                object_id=str(file_id),
                part_number=part_no,
            )
        except object_storage.MultiPartUploadNotFoundError as err:
            error = self.S3UploadNotFoundError(
                s3_upload_id=s3_upload_id, bucket_id=bucket_id
            )
            log.error(
                error,
                exc_info=True,
                extra={
                    "s3_upload_id": s3_upload_id,
                    "bucket_id": bucket_id,
                    "file_id": file_id,
                    "part_no": part_no,
                    "storage_alias": storage_alias,
                },
            )
            raise error from err

    async def complete_file_upload(self, *, box_id: UUID4, file_id: UUID4) -> None:
        """Instruct S3 to complete a multipart upload.

        Raises:
        - `FileUploadNotFound` if the FileUpload isn't found.
        - `S3UploadDetailsNotFoundError` if the S3UploadDetails aren't found.
        - `BoxNotFoundError` if the FileUploadBox isn't found.
        - `LockedBoxError` if the box exists but is locked.
        - `UnknownStorageAliasError` if the storage alias is not known.
        - `UploadCompletionError` if there's an error while telling S3 to complete the upload.
        """
        # Get the FileUpload from the DB
        try:
            file_upload = await self._file_upload_dao.get_by_id(file_id)
        except ResourceNotFoundError as err:
            error = self.FileUploadNotFound(file_id=file_id)
            log.error(error, extra={"box_id": box_id, "file_id": file_id})
            raise error from err

        # Mark the FileUpload as complete
        if file_upload.completed:
            log.info("FileUpload with ID %s already marked complete.", file_id)
            return
        file_upload.completed = True

        # Get s3 upload details
        try:
            s3_upload_details = await self._s3_upload_details_dao.get_by_id(file_id)
        except ResourceNotFoundError as err:
            error = self.S3UploadDetailsNotFoundError(file_id=file_id)
            log.error(error, extra={"box_id": box_id, "file_id": file_id})
            raise error from err
        storage_alias = s3_upload_details.storage_alias
        s3_upload_id = s3_upload_details.s3_upload_id

        # Update the FileUploadBox with new size and file count
        box = await self._get_unlocked_box(box_id=box_id)
        box.size += file_upload.size
        box.file_count += 1

        # Complete the s3 multipart upload
        bucket_id, object_storage = self._get_bucket_and_storage(storage_alias)
        try:
            await object_storage.complete_multipart_upload(
                upload_id=s3_upload_id,
                bucket_id=bucket_id,
                object_id=str(file_id),
            )
            log.info(
                "S3 multipart upload %s completed for file %s",
                s3_upload_id,
                file_id,
                extra={
                    "bucket_id": bucket_id,
                    "storage_alias": storage_alias,
                    "box_id": box_id,
                },
            )
        except object_storage.MultiPartUploadConfirmError as err:
            # This usually can't be repaired, so abort the upload attempt
            error = self.UploadCompletionError(
                file_id=file_id, s3_upload_id=s3_upload_id, bucket_id=bucket_id
            )
            log.error(
                error,
                exc_info=True,
                extra={
                    "file_id": file_id,
                    "bucket_id": bucket_id,
                    "storage_alias": storage_alias,
                    "s3_upload_id": s3_upload_id,
                    "box_id": box_id,
                },
            )
            raise error from err

        # Update local collections now that S3 upload is successfully completed
        await self._file_upload_box_dao.update(box)
        await self._file_upload_dao.update(file_upload)
        s3_upload_details.completed = now_utc_ms_prec()
        await self._s3_upload_details_dao.update(s3_upload_details)
        log.debug("DB data updated for upload completion of file %s", file_id)

    async def remove_file_upload(self, *, box_id: UUID4, file_id: UUID4) -> None:
        """Remove a file upload and cancel the ongoing upload if applicable.

        Raises:
        - `BoxNotFoundError` if the box does not exist.
        - `LockedBoxError` if the box exists but is locked.
        - `S3UploadDetailsNotFoundError` if the S3UploadDetails aren't found.
        - `UnknownStorageAliasError` if the storage alias is not known.
        - `UploadAbortError` if there's an error instructing S3 to abort the upload.
        """
        # Make sure box exists and is unlocked (unless overridden)
        box = await self._get_unlocked_box(box_id=box_id)

        # Retrieve the FileUpload data
        try:
            file_upload = await self._file_upload_dao.get_by_id(file_id)
        except ResourceNotFoundError:
            log.info("File %s not found - presumed already deleted.", file_id)
            return

        try:
            s3_upload_details = await self._s3_upload_details_dao.get_by_id(file_id)
        except ResourceNotFoundError as err:
            error = self.S3UploadDetailsNotFoundError(file_id=file_id)
            log.error(error, extra={"box_id": box_id, "file_id": file_id})
            raise error from err

        if file_upload.completed:
            await self._remove_completed_file_upload(
                s3_upload_details=s3_upload_details
            )
            box.file_count -= 1
            box.size -= file_upload.size
            await self._file_upload_box_dao.update(box)
        else:
            await self._remove_incomplete_file_upload(
                s3_upload_details=s3_upload_details
            )

        await self._s3_upload_details_dao.delete(file_id)
        await self._file_upload_dao.delete(file_id)
        log.info("File %s deleted from box %s", file_id, box_id)

    async def create_file_upload_box(
        self, *, box_id: UUID4, storage_alias: str
    ) -> None:
        """Create a new FileUploadBox with the given ID and S3 storage alias.

        Raises:
        - `BoxAlreadyExistsError` if there's already a FileUploadBox with the same ID.
        - `UnknownStorageAliasError` if the storage alias is not known.
        """
        # TODO: Decide if we're using the same IDs in UCS/UOS or using random IDs!
        if storage_alias not in self._config.object_storages:
            raise self.UnknownStorageAliasError(storage_alias=storage_alias)

        box = FileUploadBox(id=box_id, storage_alias=storage_alias)

        try:
            await self._file_upload_box_dao.insert(box)
        except ResourceAlreadyExistsError as err:
            error = self.BoxAlreadyExistsError(box_id=box_id)
            log.error(error, extra={"box_id": box_id, "storage_alias": storage_alias})
            raise error from err

    async def lock_file_upload_box(self, *, box_id: UUID4) -> None:
        """Lock an existing FileUploadBox.

        Raises:
        - `BoxNotFoundError` if the FileUploadBox isn't found in the DB.
        - `IncompleteUploadsError` if the FileUploadBox has incomplete FileUploads.
        """
        try:
            box = await self._file_upload_box_dao.get_by_id(box_id)
        except ResourceNotFoundError as err:
            error = self.BoxNotFoundError(box_id=box_id)
            log.error(error)
            raise error from err

        incomplete_files_cursor = self._file_upload_dao.find_all(
            mapping={"box_id": box_id, "completed": False}
        )
        file_ids = sorted([x.id async for x in incomplete_files_cursor])
        if file_ids:
            error = self.IncompleteUploadsError(box_id=box_id, file_ids=file_ids)
            log.error(error, extra={"box_id": box_id, "file_ids": str(file_ids)})
            raise error

        if not box.locked:
            box.locked = True
            await self._file_upload_box_dao.update(box)
        log.info("Locked box with ID %s", box_id)

    async def unlock_file_upload_box(self, *, box_id: UUID4) -> None:
        """Unlock an existing FileUploadBox.

        Raises:
        - `BoxNotFoundError` if the FileUploadBox isn't found in the DB.
        """
        try:
            box = await self._file_upload_box_dao.get_by_id(box_id)
        except ResourceNotFoundError as err:
            error = self.BoxNotFoundError(box_id=box_id)
            log.error(error)
            raise error from err

        box.locked = False
        await self._file_upload_box_dao.update(box)
        log.info("Unlocked box with ID %s", box_id)

    async def get_file_ids_for_box(self, *, box_id: UUID4) -> list[UUID4]:
        """Return the list of file IDs for a FileUploadBox.

        Raises:
        - `BoxNotFoundError` if the FileUploadBox isn't found in the DB.
        """
        try:
            # assert the box exists
            _ = await self._file_upload_box_dao.get_by_id(box_id)
        except ResourceNotFoundError as err:
            error = self.BoxNotFoundError(box_id=box_id)
            log.error(error)
            raise error from err

        # Box exists, now get all file uploads
        file_ids = [
            x.id
            async for x in self._file_upload_dao.find_all(
                mapping={"box_id": box_id, "completed": True}
            )
        ]
        return file_ids
