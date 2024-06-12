# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""DAO interface for accessing the database."""

from abc import ABC, abstractmethod

from hexkit.protocols.dao import DaoNaturalId, ResourceNotFoundError  # noqa: F401

from ifrs.core import models

# port described by a type alias:
FileMetadataDaoPort = DaoNaturalId[models.FileMetadata]
NonStagedFileRequestedDaoPort = DaoNaturalId[models.NonStagedFileRequestedRecord]
FileUploadValidationSuccessDaoPort = DaoNaturalId[
    models.FileUploadValidationSuccessRecord
]
FileDeletionRequestedDaoPort = DaoNaturalId[models.FileDeletionRequestedRecord]


class OutboxDaoCollectionPort(ABC):
    """Interface for the DAOs of the outbox collection."""

    @abstractmethod
    def get_file_deletion_requested_dao(
        self,
    ) -> DaoNaturalId[models.FileDeletionRequestedRecord]:
        """Get the DAO for the file deletion requested records."""
        ...

    @abstractmethod
    def get_file_upload_validation_success_dao(
        self,
    ) -> DaoNaturalId[models.FileUploadValidationSuccessRecord]:
        """Get the DAO for the file upload validation success records."""
        ...

    @abstractmethod
    def get_nonstaged_file_requested_dao(
        self,
    ) -> DaoNaturalId[models.NonStagedFileRequestedRecord]:
        """Get the DAO for the non-staged file requested records."""
        ...
