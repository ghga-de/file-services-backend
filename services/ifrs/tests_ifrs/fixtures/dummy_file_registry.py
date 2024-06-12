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
"""Contains a mock FileRegistry class for testing."""

from ghga_event_schemas.pydantic_ import (
    FileDeletionRequested,
    FileUploadValidationSuccess,
    NonStagedFileRequested,
)
from ifrs.core import models
from ifrs.ports.inbound.file_registry import FileRegistryPort


class DummyFileRegistry(FileRegistryPort):
    """Dummy file registry for testing purposes."""

    def __init__(self):
        self.last_call = None

    async def register_file(
        self,
        *,
        file_without_object_id: models.FileMetadataBase,
        staging_object_id: str,
        staging_bucket_id: str,
    ) -> None:
        """Here to satisfy ABC"""
        pass

    async def stage_registered_file(
        self,
        *,
        file_id: str,
        decrypted_sha256: str,
        outbox_object_id: str,
        outbox_bucket_id: str,
    ) -> None:
        """Here to satisfy ABC"""
        pass

    async def delete_file(self, *, file_id: str) -> None:
        """Here to satisfy ABC"""
        pass

    async def upsert_nonstaged_file_requested(
        self, *, resource_id: str, update: NonStagedFileRequested
    ) -> None:
        """Mock method to capture calls from outbox subscriber."""
        self.last_call = "upsert_nonstaged_file_requested"

    async def upsert_file_deletion_requested(
        self, *, resource_id: str, update: FileDeletionRequested
    ) -> None:
        """Mock method to capture calls from outbox subscriber."""
        self.last_call = "upsert_file_deletion_requested"

    async def upsert_file_upload_validation_success(
        self, *, resource_id: str, update: FileUploadValidationSuccess
    ) -> None:
        """Mock method to capture calls from outbox subscriber."""
        self.last_call = "upsert_file_upload_validation_success"
