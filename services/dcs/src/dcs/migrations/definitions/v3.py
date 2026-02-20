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

from typing import Any

from hexkit.providers.mongodb.migrations import (
    Document,
    MigrationDefinition,
    Reversible,
)
from hexkit.providers.mongokafka.provider.persistent_pub import PersistentKafkaEvent

from dcs.core.models import AccessTimeDrsObject

DRS_OBJECTS = "drs_objects"
DCS_PERSISTED_EVENTS = "dcsPersistedEvents"


# TODO: Update Study Repository/Accession Store with accession - file ID mappings!


# TODO: Remove DRS URI from stored drs objects
# TODO: Remove drs_uri from file registered for download events
class V3Migration(MigrationDefinition, Reversible):
    """Rename fields to align with updated event schemas and consistency across services.

    Affected collections:
    - drs_objects (AccessTimeDrsObject)
        - Rename `decryption_secret_id` to `secret_id`
        - Rename `s3_endpoint_alias` to `storage_alias`
        - Rename `object_id` to `file_id`
    - dcsPersistedEvents
        - Where type_ == 'download_served':
            - In payload, rename `s3_endpoint_alias` to `storage_alias`
            - In payload, rename `object_id` to `file_id`

    This migration can be reversed by renaming the fields back to their original names.
    """

    version = 3

    async def apply(self):
        """Perform the migration."""

        async def rename_drs_object_fields(doc: Document) -> Document:
            """Rename fields in DRS object documents."""
            if "decryption_secret_id" in doc:
                doc["secret_id"] = doc.pop("decryption_secret_id")
            if "s3_endpoint_alias" in doc:
                doc["storage_alias"] = doc.pop("s3_endpoint_alias")
            if "object_id" in doc:
                doc["file_id"] = doc.pop("object_id")
            return doc

        async def rename_persisted_event_fields(doc: Document) -> Document:
            """Rename fields in persisted event payload where applicable."""
            accession = doc["key"]  # all events in DCS have accession for key
            doc["_id"] = doc["_id"].replace(accession, "")
            payload: dict[str, Any] = doc.get("payload", {})
            if "s3_endpoint_alias" in payload:
                payload["storage_alias"] = payload.pop("s3_endpoint_alias")
            if "object_id" in payload:
                payload["file_id"] = payload.pop("object_id")
            doc["payload"] = payload

            return doc

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

    async def unapply(self):
        """Revert the migration."""

        async def revert_drs_object_fields(doc: Document) -> Document:
            """Revert field names in DRS object documents."""
            if "secret_id" in doc:
                doc["decryption_secret_id"] = doc.pop("secret_id")
            if "storage_alias" in doc:
                doc["s3_endpoint_alias"] = doc.pop("storage_alias")
            if "file_id" in doc:
                doc["object_id"] = doc.pop("file_id")
            return doc

        async def revert_persisted_event_fields(doc: Document) -> Document:
            """Revert field names in persisted event payload where applicable."""
            payload: dict[str, Any] = doc.get("payload", {})
            type_ = doc.get("type_")

            # Only update download_served events
            if type_ == "download_served":
                if "storage_alias" in payload:
                    payload["s3_endpoint_alias"] = payload.pop("storage_alias")
                if "file_id" in payload:
                    payload["object_id"] = payload.pop("file_id")
                doc["payload"] = payload

            return doc

        async with self.auto_finalize(
            coll_names=[DRS_OBJECTS, DCS_PERSISTED_EVENTS], copy_indexes=True
        ):
            await self.migrate_docs_in_collection(
                coll_name=DRS_OBJECTS,
                change_function=revert_drs_object_fields,
            )
            await self.migrate_docs_in_collection(
                coll_name=DCS_PERSISTED_EVENTS,
                change_function=revert_persisted_event_fields,
            )
