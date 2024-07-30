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
"""KafkaEventSubscriber receiving events from UCS and validating file uploads"""

import logging

from ghga_event_schemas import pydantic_ as event_schemas
from ghga_event_schemas.validation import get_validated_payload
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.daosub import DaoSubscriberProtocol
from hexkit.protocols.eventsub import EventSubscriberProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from irs.core.models import InterrogationSubject
from irs.ports.inbound.interrogator import InterrogatorPort

log = logging.getLogger(__name__)


class EventSubTranslatorConfig(BaseSettings):
    """Config for publishing file upload-related events."""

    file_registered_event_topic: str = Field(
        default=...,
        description="Name of the topic used for events indicating that a new file has"
        + " been internally registered.",
        examples=["internal-file-registry"],
    )
    file_registered_event_type: str = Field(
        default=...,
        description="The type used for events indicating that a new file has"
        + " been internally registered.",
        examples=["file_registered"],
    )


class EventSubTranslator(EventSubscriberProtocol):
    """A triple hexagonal translator compatible with the EventSubscriberProtocol that
    is used to received events relevant for file uploads.
    """

    def __init__(
        self,
        config: EventSubTranslatorConfig,
        interrogator: InterrogatorPort,
    ):
        """Initialize with config parameters and core dependencies."""
        self._config = config
        self._interrogator = interrogator

        self.topics_of_interest = [config.file_registered_event_topic]
        self.types_of_interest = [config.file_registered_event_type]

    async def _consume_validated(
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii, key: str
    ) -> None:
        """
        Receive and process an event with already validated topic and type.

        Args:
            payload (JsonObject): The data/payload to send with the event.
            type_ (str): The type of the event.
            topic (str): Name of the topic the event was published to.
            key (str): The key associated with the event.
        """
        if type_ == self._config.file_registered_event_type:
            await self._consume_file_internally_registered(payload=payload)
        else:
            raise RuntimeError(f"Unexpected event of type: {type_}")

    async def _consume_file_internally_registered(self, *, payload: JsonObject):
        """
        Consume confirmation event that object data has been moved to permanent storage
        and the transient staging copy can be removed.
        """
        validated_payload = get_validated_payload(
            payload=payload,
            schema=event_schemas.FileInternallyRegistered,
        )

        await self._interrogator.remove_staging_object(
            file_id=validated_payload.file_id,
            storage_alias=validated_payload.s3_endpoint_alias,
        )


class OutboxSubTranslatorConfig(BaseSettings):
    """Config for the outbox subscriber"""

    upload_received_event_topic: str = Field(
        default=...,
        description="Name of the topic to publish events that inform about new file uploads.",
        examples=["uploads", "file-uploads"],
    )


class FileUploadReceivedSubTranslator(
    DaoSubscriberProtocol[event_schemas.FileUploadReceived]
):
    """A triple hexagonal translator compatible with the DaoSubscriberProtocol that
    is used to received events relevant for file uploads.
    """

    event_topic: str
    dto_model = event_schemas.FileUploadReceived

    def __init__(
        self,
        *,
        config: OutboxSubTranslatorConfig,
        interrogator: InterrogatorPort,
    ):
        self._interrogator = interrogator
        self.event_topic = config.upload_received_event_topic

    async def changed(
        self, resource_id: str, update: event_schemas.FileUploadReceived
    ) -> None:
        """Consume upsertion event for a FileUploadReceived event schema.

        Idempotence is handled by a 'fingerprinting' mechanism in the Interrogator.
        """
        subject = InterrogationSubject(
            file_id=update.file_id,
            inbox_bucket_id=update.bucket_id,
            inbox_object_id=update.object_id,
            storage_alias=update.s3_endpoint_alias,
            decrypted_size=update.decrypted_size,
            expected_decrypted_sha256=update.expected_decrypted_sha256,
            upload_date=update.upload_date,
            submitter_public_key=update.submitter_public_key,
        )
        await self._interrogator.interrogate(subject=subject)

    async def deleted(self, resource_id: str) -> None:
        """Consume event indicating the deletion of the event -- only log a warning."""
        log.warning(
            "Received DELETED-type event for FileUploadReceived with resource ID '%s'",
            resource_id,
        )
