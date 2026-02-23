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

"""Migration Definition for moving to DB version 3"""

from hashlib import sha256
from typing import Any
from uuid import UUID

from hexkit.providers.mongodb.migrations import Document, MigrationDefinition
from hexkit.providers.mongokafka.provider.persistent_pub import PersistentKafkaEvent

from dcs.core.models import AccessTimeDrsObject

__all__ = ["DCS_PERSISTED_EVENTS", "DRS_OBJECTS", "V3Migration"]

DRS_OBJECTS = "drs_objects"
DCS_PERSISTED_EVENTS = "dcsPersistedEvents"


def derive_file_id_from_accession(accession: str) -> UUID:
    """Use the first portion of the SHA256 hash of an accession to derive a UUID4"""
    hash = sha256(accession.encode()).hexdigest()
    uuid_str = f"{hash[0:8]}-{hash[8:12]}-4{hash[13:16]}-a{hash[17:20]}-{hash[20:32]}"
    return UUID(uuid_str)


async def rename_drs_object_fields(doc: Document) -> Document:
    """Rename fields in DRS object documents."""
    if "decryption_secret_id" in doc:
        doc["secret_id"] = doc.pop("decryption_secret_id")
        doc["storage_alias"] = doc.pop("s3_endpoint_alias")
        doc["_id"] = derive_file_id_from_accession(doc["_id"])
    return doc


async def rename_persisted_event_fields(doc: Document) -> Document:
    """Rename fields in persisted event payload where applicable."""
    accession = doc["key"]  # all events in DCS have accession for key
    uuid4_file_id = derive_file_id_from_accession(accession)

    # Update the compaction_key field (_id). All stored topics here are compacted.
    doc["_id"] = doc["_id"].replace(accession, str(uuid4_file_id))

    # Update the event key - replacing the accession with the file ID
    doc["key"] = str(uuid4_file_id)
    doc["published"] = False

    # The following will apply to different event types, but it is not necessary to
    #  evaluate the event type. We can just go about it on a field-by-field basis.
    payload: dict[str, Any] = doc.get("payload", {})
    if "s3_endpoint_alias" in payload:
        payload["storage_alias"] = payload.pop("s3_endpoint_alias")
    if "object_id" in payload:
        payload["file_id"] = payload.pop("object_id")
    # Get rid of the DRS URI in the drs_object_registered event (not helpful anyway)
    _ = payload.pop("drs_uri", None)

    doc["payload"] = payload

    return doc


class V3Migration(MigrationDefinition):
    """Rename fields to align with updated event schemas and consistency across services.

    Affected collections:
    - drs_objects (AccessTimeDrsObject)
        - Hash accession in `_id` to a UUID4, just like in the IFRS.
        - Rename `decryption_secret_id` to `secret_id`
        - Rename `s3_endpoint_alias` to `storage_alias`
    - dcsPersistedEvents
        - all types:
            - derive uuid4 file ID from GHGA accession hash - reference the key field
            - set event `key` to the new uuid4 file ID
            - in the compaction_key field (_id), replace the accession with the uuid4 ID
            - set published to False
            - in payload, set file_id to the new uuid4 ID (all stored events have it)
        - Where type_ == 'drs_object_registered':
            - rename `upload_date` to `archive_date`
            - remove `drs_uri`  (accession isn't known to DCS at that point in the flow)
        - Where type_ == 'drs_object_served':
            - In payload, rename `s3_endpoint_alias` to `storage_alias`
    This migration is not reversible.
    """

    version = 3

    async def apply(self):
        """Perform the migration."""
        # First ascertain whether the Study Repository is online. Cannot proceed until then.
        # TODO: Do that ^

        async with self.auto_finalize(
            coll_names=[DRS_OBJECTS, DCS_PERSISTED_EVENTS], copy_indexes=True
        ):
            await self.migrate_docs_in_collection(
                coll_name=DRS_OBJECTS,
                change_function=rename_drs_object_fields,
                validation_model=AccessTimeDrsObject,
                id_field="accession",
            )

            await self.migrate_docs_in_collection(
                coll_name=DCS_PERSISTED_EVENTS,
                change_function=rename_persisted_event_fields,
                validation_model=PersistentKafkaEvent,
                id_field="compaction_key",
            )
