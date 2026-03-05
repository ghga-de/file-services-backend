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

"""S3-based Implementation of object storage adapters."""

import logging

from ghga_service_commons.utils.multinode_storage import ObjectStorages
from hexkit.protocols.objstorage import ObjectStorageProtocol

from ucs.config import Config
from ucs.core.models import FileUpload, S3UploadDetails
from ucs.ports.outbound.storage import S3ClientPort

log = logging.getLogger(__name__)


class S3Client(S3ClientPort):
    """A class that isolates S3 logic and error handling from the core"""

    def __init__(self, *, config: Config, object_storages: ObjectStorages):
        self._config = config
        self._object_storages = object_storages

    def _get_storage_for_alias(self, storage_alias: str) -> ObjectStorageProtocol:
        """Return the bucket ID and ObjectStorageProtocol for a given storage alias.

        Raises `UnknownStorageAliasError` if the storage alias is not known.
        """
        try:
            _, object_storage = self._object_storages.for_alias(storage_alias)
        except KeyError as error:
            unknown_alias = self.UnknownStorageAliasError(storage_alias=storage_alias)
            log.error(unknown_alias, extra={"storage_alias": storage_alias})
            raise unknown_alias from error
        log.debug(
            "Found bucket '%s' and object storage for alias '%s'",
            _,
            storage_alias,
        )
        return object_storage

    def get_bucket_id_for_alias(self, *, storage_alias: str) -> str:
        """Retrieve the bucket ID for a given storage alias.

        Raises `UnknownStorageAliasError` if the storage alias is not known.
        """
        try:
            bucket_id, _ = self._object_storages.for_alias(storage_alias)
        except KeyError as error:
            unknown_alias = self.UnknownStorageAliasError(storage_alias=storage_alias)
            log.error(unknown_alias, extra={"storage_alias": storage_alias})
            raise unknown_alias from error
        log.debug(
            "Found bucket '%s' and object storage for alias '%s'",
            bucket_id,
            storage_alias,
        )
        return bucket_id

    async def init_multipart_upload(self, *, file_upload: FileUpload) -> str:
        """Initiate a new multipart upload for a FileUpload.

        Returns a str containing the multipart upload ID.

        Raises:
            `UnknownStorageAliasError` if the storage alias is not known.
            `OrphanedMultipartUploadError` if an S3 upload is already in progress.
        """
        bucket_id = file_upload.bucket_id
        object_id = file_upload.object_id

        # Get the storage
        object_storage = self._get_storage_for_alias(
            storage_alias=file_upload.storage_alias
        )

        # Initiate a new multipart file upload on the S3 instance
        try:
            s3_upload_id = await object_storage.init_multipart_upload(
                bucket_id=bucket_id, object_id=str(object_id)
            )
            log.info(
                "S3 multipart upload %s created for file ID %s (file alias %s)",
                s3_upload_id,
                file_upload.id,
                file_upload.alias,
            )
            return s3_upload_id
        except object_storage.MultiPartUploadAlreadyExistsError as err:
            #  See the long note on UploadController.initiate_file_upload()
            raise self.OrphanedMultipartUploadError(
                file_id=file_upload.id, bucket_id=bucket_id
            ) from err

    async def get_part_upload_url(
        self, *, s3_upload_details: S3UploadDetails, part_no: int
    ) -> str:
        """Get a pre-signed upload URL for the file."""
        # Get the storage
        object_storage = self._get_storage_for_alias(
            storage_alias=s3_upload_details.storage_alias
        )
        try:
            return await object_storage.get_part_upload_url(
                upload_id=s3_upload_details.s3_upload_id,
                bucket_id=s3_upload_details.bucket_id,
                object_id=str(s3_upload_details.object_id),
                part_number=part_no,
            )
        except object_storage.MultiPartUploadNotFoundError as err:
            raise self.S3UploadNotFoundError(
                s3_upload_id=s3_upload_details.s3_upload_id,
                bucket_id=s3_upload_details.bucket_id,
            ) from err
