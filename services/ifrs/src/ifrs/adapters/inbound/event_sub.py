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

"""Adapter for receiving events providing metadata on files"""

from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.protocols.daosub import DaoSubscriberProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from ifrs.ports.inbound.file_registry import FileRegistryPort


class OutboxSubTranslatorConfig(BaseSettings):
    """Config for the outbox subscriber"""

    files_to_delete_topic: str = Field(
        ...,
        description="The name of the topic to receive events informing about files to delete.",
        examples=["file_deletions"],
    )
    files_to_delete_type: str = Field(
        ...,
        description="The type used for events informing about a file to be deleted.",
        examples=["file_deletion_requested"],
    )
    files_to_register_topic: str = Field(
        ...,
        description="The name of the topic to receive events informing about new files "
        + "to register.",
        examples=["file_interrogation"],
    )
    files_to_register_type: str = Field(
        ...,
        description="The type used for events informing about new files to register.",
        examples=["file_interrogation_success"],
    )
    files_to_stage_topic: str = Field(
        ...,
        description="The name of the topic to receive events informing about files to stage.",
        examples=["file_downloads"],
    )
    files_to_stage_type: str = Field(
        ...,
        description="The type used for events informing about a file to be staged.",
        examples=["file_stage_requested"],
    )


class NonstagedFileRequestedListener(
    DaoSubscriberProtocol[event_schemas.NonStagedFileRequested]
):
    """A class that consumes NonStagedFileRequested events."""

    event_topic: str
    dto_model = event_schemas.NonStagedFileRequested

    def __init__(
        self,
        *,
        config: OutboxSubTranslatorConfig,
        file_registry: FileRegistryPort,
    ):
        self._config = config
        self._file_registry = file_registry
        self.event_topic = config.files_to_stage_topic

    async def changed(
        self, resource_id: str, update: event_schemas.NonStagedFileRequested
    ) -> None:
        """Consume change event (created or updated) for download request data."""
        await self._file_registry.upsert_nonstaged_file_requested(
            resource_id=resource_id, update=update
        )

    async def deleted(self, resource_id: str) -> None:
        """Consume event indicating the deletion of a NonStagedFileRequested event."""
        pass


class FileDeletionRequestedListener(
    DaoSubscriberProtocol[event_schemas.FileDeletionRequested]
):
    """A class that consumes FileDeletionRequested events."""

    event_topic: str
    dto_model = event_schemas.FileDeletionRequested

    def __init__(
        self,
        *,
        config: OutboxSubTranslatorConfig,
        file_registry: FileRegistryPort,
    ):
        self._config = config
        self._file_registry = file_registry
        self.event_topic = config.files_to_delete_topic

    async def changed(
        self, resource_id: str, update: event_schemas.FileDeletionRequested
    ) -> None:
        """Consume change event (created or updated) for File Deletion Requests."""
        await self._file_registry.upsert_file_deletion_requested(
            resource_id=resource_id, update=update
        )

    async def deleted(self, resource_id: str) -> None:
        """Consume event indicating the deletion of a File Deletion Request."""
        pass


class FileValidationSuccessListener(
    DaoSubscriberProtocol[event_schemas.FileUploadValidationSuccess]
):
    """A class that consumes FileDeletionRequested events."""

    event_topic: str
    dto_model = event_schemas.FileUploadValidationSuccess

    def __init__(
        self,
        *,
        config: OutboxSubTranslatorConfig,
        file_registry: FileRegistryPort,
    ):
        self._config = config
        self._file_registry = file_registry
        self.event_topic = config.files_to_register_topic

    async def changed(
        self, resource_id: str, update: event_schemas.FileUploadValidationSuccess
    ) -> None:
        """Consume change event (created or updated) for FileUploadValidationSuccess events."""
        await self._file_registry.upsert_file_upload_validation_success(
            resource_id=resource_id, update=update
        )

    async def deleted(self, resource_id: str) -> None:
        """Consume event indicating the deletion of a FileUploadValidationSuccess events."""
        pass
