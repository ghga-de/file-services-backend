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

"""Interfaces for object storage adapters and the exception they may throw."""

from abc import ABC, abstractmethod

from hexkit.protocols.objstorage import (  # noqa: F401
    ObjectStorageProtocol as ObjectStoragePort,
)
from pydantic import UUID4

from ucs.core.models import FileUpload


# TODO: Add class comparison script to ucs/scripts
class S3ClientPort(ABC):
    """A class that isolates S3 logic and error handling from the core"""

    class OrphanedMultipartUploadError(RuntimeError):
        """Raised when a pre-existing multipart upload is unexpectedly found"""

        def __init__(self, *, file_id: UUID4, bucket_id: str):
            msg = (
                f"An S3 multipart upload already exists for file ID {file_id} and"
                + f" bucket ID {bucket_id}."
            )
            super().__init__(msg)

    class UnknownStorageAliasError(RuntimeError):
        """Thrown when the requested storage location is not configured.
        The given parameter given should be a configured alias, but is not.
        """

        def __init__(self, *, storage_alias: str):
            message = f"No storage node exists for alias {storage_alias}."
            super().__init__(message)

    @abstractmethod
    def get_bucket_id_for_alias(self, *, storage_alias: str) -> str:
        """Retrieve the bucket ID for a given storage alias.

        Raises `UnknownStorageAliasError` if the storage alias is not known.
        """

    @abstractmethod
    async def init_multipart_upload(self, *, file_upload: FileUpload) -> str:
        """Initiate a new multipart upload for a FileUpload.

        Returns a str containing the multipart upload ID.

        Raises:
            `UnknownStorageAliasError` if the storage alias is not known.
            `OrphanedMultipartUploadError` if an S3 upload is already in progress.
        """
