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

"""Receive events informing about files that are expected to be uploaded."""

import logging

from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.protocols.daosub import DaoSubscriberProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from fins.ports.inbound.information_service import InformationServicePort

log = logging.getLogger(__name__)


class OutboxSubTranslatorConfig(BaseSettings):
    """Config for the outbox subscriber"""

    files_to_delete_topic: str = Field(
        default=...,
        description="The name of the topic for events informing about files to be deleted.",
        examples=["file_deletions"],
    )
    files_to_register_topic: str = Field(
        default=...,
        description="The name of the topic for events informing about new registered files"
        " for which the metadata should be made available.",
        examples=["file_registrations"],
    )


class InformationRegistrationListener(
    DaoSubscriberProtocol[event_schemas.FileInternallyRegistered]
):
    """A class that consumes FileInternallyRegistered events and persists relevant information."""

    event_topic: str
    dto_model: event_schemas.FileInternallyRegistered

    def __init__(
        self,
        *,
        config: OutboxSubTranslatorConfig,
        information_service: InformationServicePort,
    ):
        self.event_topic = config.files_to_register_topic
        self.information_service = information_service

    async def changed(
        self, resource_id: str, update: event_schemas.FileInternallyRegistered
    ) -> None:
        """Consume change event for File Internally Registered.
        Idempotence is handled by the core, so no intermediary is required.
        """
        self.information_service.register_information(update)

    async def deleted(self, resource_id: str) -> None:
        """Consume event indicating the deletion of a File Internally Registered Request."""
        log.warning(
            "Received DELETED-type event for FileInternallyRegistered with resource ID '%s'",
            resource_id,
        )


class InformationDeletionRequestedListener(
    DaoSubscriberProtocol[event_schemas.FileDeletionRequested]
):
    """A class that consumes FileDeletionRequested events."""

    event_topic: str
    dto_model = event_schemas.FileDeletionRequested

    def __init__(
        self,
        *,
        config: OutboxSubTranslatorConfig,
        information_service: InformationServicePort,
    ):
        self.event_topic = config.files_to_delete_topic
        self.information_service = information_service

    async def changed(
        self, resource_id: str, update: event_schemas.FileDeletionRequested
    ) -> None:
        """Consume change event for File Deletion Requests.
        Idempotence is handled by the core, so no intermediary is required.
        """
        self.information_service.deletion_requested(file_id=update.file_id)

    async def deleted(self, resource_id: str) -> None:
        """Consume event indicating the deletion of a File Deletion Request."""
        log.warning(
            "Received DELETED-type event for FileDeletionRequested with resource ID '%s'",
            resource_id,
        )
