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

import logging

from ghga_event_schemas import pydantic_ as event_schemas
from ghga_event_schemas.validation import get_validated_payload
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka.provider.daosub import CHANGE_EVENT_TYPE, DELETE_EVENT_TYPE
from pydantic import Field
from pydantic_settings import BaseSettings

from ifrs.core.models import FileMetadataBase
from ifrs.ports.inbound.file_registry import FileRegistryPort

log = logging.getLogger(__name__)


class EventSubTranslatorConfig(BaseSettings):
    """Config for the event subscriber"""

    files_to_delete_topic: str = Field(
        default=...,
        description="The name of the topic to receive events informing about files to delete.",
        examples=["file-deletions"],
    )
    files_to_register_topic: str = Field(
        default=...,
        description="The name of the topic to receive events informing about new files "
        + "to register.",
        examples=["file-interrogations"],
    )
    files_to_stage_topic: str = Field(
        default=...,
        description="The name of the topic to receive events informing about files to stage.",
        examples=["file-downloads"],
    )


class EventSubTranslator(EventSubscriberProtocol):
    """A triple hexagonal translator compatible with the EventSubscriberProtocol that
    is used to received new files to register as DRS objects.

    The inbound events are published according to the outbox pattern, meaning that the
    type will be 'upserted' for each event, and that the topic will correspond
    to a single event type. The 'upserted' value is defined in `hexkit`.
    """

    def __init__(
        self,
        config: EventSubTranslatorConfig,
        file_registry: FileRegistryPort,
    ):
        """Initialize with config parameters and core dependencies."""
        self.topics_of_interest = [
            config.files_to_stage_topic,
            config.files_to_delete_topic,
            config.files_to_register_topic,
        ]
        self.types_of_interest = [CHANGE_EVENT_TYPE]

        self._file_registry = file_registry
        self._config = config

    async def _consume_file_upload_validation_success(self, payload: JsonObject):
        validated_payload = get_validated_payload(
            payload=payload, schema=event_schemas.FileUploadValidationSuccess
        )
        file_without_object_id = FileMetadataBase(
            file_id=validated_payload.file_id,
            decrypted_sha256=validated_payload.decrypted_sha256,
            decrypted_size=validated_payload.decrypted_size,
            upload_date=validated_payload.upload_date,
            decryption_secret_id=validated_payload.decryption_secret_id,
            encrypted_part_size=validated_payload.encrypted_part_size,
            encrypted_parts_md5=validated_payload.encrypted_parts_md5,
            encrypted_parts_sha256=validated_payload.encrypted_parts_sha256,
            content_offset=validated_payload.content_offset,
            storage_alias=validated_payload.s3_endpoint_alias,
        )

        await self._file_registry.register_file(
            file_without_object_id=file_without_object_id,
            staging_object_id=validated_payload.object_id,
            staging_bucket_id=validated_payload.bucket_id,
        )

    async def _consume_file_deletion_requested(self, payload: JsonObject):
        validated_payload = get_validated_payload(
            payload=payload, schema=event_schemas.FileDeletionRequested
        )
        await self._file_registry.delete_file(file_id=validated_payload.file_id)

    async def _consume_nonstaged_file_requested(self, payload: JsonObject):
        validated_payload = get_validated_payload(
            payload=payload, schema=event_schemas.NonStagedFileRequested
        )
        await self._file_registry.stage_registered_file(
            file_id=validated_payload.file_id,
            decrypted_sha256=validated_payload.decrypted_sha256,
            outbox_object_id=validated_payload.target_object_id,
            outbox_bucket_id=validated_payload.target_bucket_id,
        )

    async def _consume_validated(
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii, key: str
    ) -> None:
        """Consume events from the topics of interest."""
        if type_ == DELETE_EVENT_TYPE:
            log.warning(
                "Received DELETED-type event for topic %s with resource ID '%s'",
                topic,
                key,
            )
            return

        match topic:
            case self._config.files_to_stage_topic:
                await self._consume_nonstaged_file_requested(payload=payload)
            case self._config.files_to_register_topic:
                await self._consume_file_upload_validation_success(payload=payload)
            case self._config.files_to_delete_topic:
                await self._consume_file_deletion_requested(payload=payload)
            case _:
                raise RuntimeError(f"Unexpected event of type: {type_}")
