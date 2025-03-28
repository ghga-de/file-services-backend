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

"""Adapter for receiving events providing metadata on files"""

import logging

from ghga_event_schemas import pydantic_ as event_schemas
from ghga_event_schemas.configs import (
    FileDeletionRequestEventsConfig,
    FileInternallyRegisteredEventsConfig,
)
from ghga_event_schemas.validation import get_validated_payload
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.daosub import DaoSubscriberProtocol
from hexkit.protocols.eventsub import EventSubscriberProtocol

from dcs.core import models
from dcs.ports.inbound.data_repository import DataRepositoryPort

log = logging.getLogger(__name__)

__all__ = [
    "EventSubTranslator",
    "EventSubTranslatorConfig",
    "FileDeletionRequestedListener",
    "OutboxSubTranslatorConfig",
]


class EventSubTranslatorConfig(FileInternallyRegisteredEventsConfig):
    """Config for receiving events providing metadata on files."""


class EventSubTranslator(EventSubscriberProtocol):
    """A triple hexagonal translator compatible with the EventSubscriberProtocol that
    is used to received new files to register as DRS objects.
    """

    def __init__(
        self,
        config: EventSubTranslatorConfig,
        data_repository: DataRepositoryPort,
    ):
        """Initialize with config parameters and core dependencies."""
        self.topics_of_interest = [config.file_internally_registered_topic]
        self.types_of_interest = [config.file_internally_registered_type]

        self._data_repository = data_repository
        self._config = config

    async def _consume_files_to_register(self, *, payload: JsonObject) -> None:
        """Consume file registration events."""
        validated_payload = get_validated_payload(
            payload=payload, schema=event_schemas.FileInternallyRegistered
        )

        file = models.DrsObjectBase(
            file_id=validated_payload.file_id,
            decryption_secret_id=validated_payload.decryption_secret_id,
            decrypted_sha256=validated_payload.decrypted_sha256,
            decrypted_size=validated_payload.decrypted_size,
            encrypted_size=validated_payload.encrypted_size,
            creation_date=validated_payload.upload_date,
            s3_endpoint_alias=validated_payload.s3_endpoint_alias,
        )

        await self._data_repository.register_new_file(file=file)

    async def _consume_validated(
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii, key: str
    ) -> None:
        """Consume events from the topics of interest."""
        if type_ == self._config.file_internally_registered_type:
            await self._consume_files_to_register(payload=payload)
        else:
            raise RuntimeError(f"Unexpected event of type: {type_}")


class OutboxSubTranslatorConfig(FileDeletionRequestEventsConfig):
    """Config for the outbox subscriber"""


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
        data_repository: DataRepositoryPort,
    ):
        self._data_repository = data_repository
        self.event_topic = config.file_deletion_request_topic

    async def changed(
        self, resource_id: str, update: event_schemas.FileDeletionRequested
    ) -> None:
        """Consume change event (created or updated) for File Deletion Requests."""
        await self._data_repository.delete_file(file_id=resource_id)

    async def deleted(self, resource_id: str) -> None:
        """Consume event indicating the deletion of a File Deletion Request."""
        log.warning(
            "Received DELETED-type event for FileDeletionRequested with resource ID '%s'",
            resource_id,
        )
