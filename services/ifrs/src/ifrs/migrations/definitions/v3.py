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

"""Database migration logic for IFRS"""

from hexkit.providers.mongodb.migrations import (
    Document,
    MigrationDefinition,
    Reversible,
)
from hexkit.providers.mongokafka.provider.persistent_pub import PersistentKafkaEvent

from ifrs.core.models import FileMetadata

# Collection names defined as constants
IFRS_PERSISTED_EVENTS = "ifrsPersistedEvents"
FILE_METADATA = "file_metadata"


class V3Migration(MigrationDefinition, Reversible):
    """Migrate select fields in the IFRS's database for the Sarcastic Fringehead epic.

    Affected data:
    - `file_metadata` collection:
      - Fields affected:
        - `_id`: renamed to `accession`
        - `upload_date`: renamed to `archive_date`
        - `decryption_secret_id`: renamed to `secret_id`
        - `content_offset`: removed
        - `encrypted_part_size`: renamed to `part_size`
        - `object_id`: renamed to `_id`
        - `object_size`: renamed to `encrypted_size`

    - `ifrsPersistedEvents` collection:
      - Fields affected:
        - `_id`: accession replaced by file ID
        - `key`: accession replaced by file ID
        - `payload.file_id`: renamed to `payload.accession`
        - `payload.object_id`: renamed to `payload.file_id`
        - `payload.upload_date`: renamed to `payload.archive_date`
        - `payload.s3_endpoint_alias`: renamed to `payload.storage_alias`
        - `payload.decryption_secret_id`: renamed to `payload.secret_id`
        - `payload.encrypted_part_size`: renamed to `payload.part_size`
        - `payload.content_offset`: deleted

    This can be reversed by renaming the fields to their original names and adding
    `content_offset` with a value of 0.
    """

    version = 3

    async def apply(self):
        """Perform the migration"""

        async def update_file_metadata(doc: Document) -> Document:
            """Update file metadata, skipping already updated docs.

            If a doc has the `accession` field, it has already been updated.
            """
            if "accession" in doc:
                return doc

            doc["accession"] = doc.pop("_id")
            doc["archive_date"] = doc.pop("upload_date")
            doc["secret_id"] = doc.pop("decryption_secret_id")
            del doc["content_offset"]
            doc["part_size"] = doc.pop("encrypted_part_size")
            doc["_id"] = doc.pop("object_id")
            doc["encrypted_size"] = doc.pop("object_size")
            return doc

        async def update_event(doc: Document) -> Document:
            """Convert a persistent event payloads for FileInternallyRegistered events."""
            payload = doc["payload"]

            # Check if already migrated by looking for payload.content_offset
            if "content_offset" not in payload:
                # this also filters out any events that are not FileInternallyRegistered
                return doc

            payload["accession"] = payload.pop("file_id")
            payload["file_id"] = payload.pop("object_id")
            payload["archive_date"] = payload.pop("upload_date")
            payload["storage_alias"] = payload.pop("s3_endpoint_alias")
            payload["secret_id"] = payload.pop("decryption_secret_id")
            payload["part_size"] = payload.pop("encrypted_part_size")
            del payload["content_offset"]
            # payload points to doc["payload"], so no need to reassign the payload field

            doc["published"] = False

            # Don't change the key or compaction_key fields

            return doc

        async with self.auto_finalize(
            coll_names=[FILE_METADATA, IFRS_PERSISTED_EVENTS], copy_indexes=False
        ):
            # migrate the file metadata collection
            await self.migrate_docs_in_collection(
                coll_name=FILE_METADATA,
                change_function=update_file_metadata,
                validation_model=FileMetadata,
                id_field="id",
            )
            # migrate persistent events and their payload fields
            await self.migrate_docs_in_collection(
                coll_name=IFRS_PERSISTED_EVENTS,
                change_function=update_event,
                validation_model=PersistentKafkaEvent,
                id_field="compaction_key",
            )

    async def unapply(self):
        """Reverse the migration.

        This will also get rid of the index over the accession field in the
        file_metadata collection.
        """

        async def revert_file_metadata(doc: Document) -> Document:
            """Revert field names and add content_offset back with value of 0."""
            if "accession" not in doc:
                return doc

            doc["object_id"] = doc.pop("_id")
            doc["_id"] = doc.pop("accession")
            doc["upload_date"] = doc.pop("archive_date")
            doc["decryption_secret_id"] = doc.pop("secret_id")
            doc["encrypted_part_size"] = doc.pop("part_size")
            doc["object_size"] = doc.pop("encrypted_size")
            doc["content_offset"] = 0
            return doc

        async def revert_event(doc: Document) -> Document:
            """Revert the payload field names"""
            payload = doc["payload"]

            # Check if already reverted by looking for payload.content_offset
            # OR if it's not a FileInternallyRegistered event  (lacks expected fields)
            if "content_offset" in payload or "accession" not in payload:
                return doc

            payload["object_id"] = payload.pop("file_id")
            payload["file_id"] = payload.pop("accession")
            payload["upload_date"] = payload.pop("archive_date")
            payload["s3_endpoint_alias"] = payload.pop("storage_alias")
            payload["decryption_secret_id"] = payload.pop("secret_id")
            payload["encrypted_part_size"] = payload.pop("part_size")
            payload["content_offset"] = 0
            # payload points to doc["payload"], so no need to reassign the payload field

            doc["published"] = False

            # Don't change the key or compaction_key fields

            return doc

        async with self.auto_finalize(
            coll_names=[FILE_METADATA, IFRS_PERSISTED_EVENTS], copy_indexes=False
        ):
            # Revert file metadata collection
            await self.migrate_docs_in_collection(
                coll_name=FILE_METADATA,
                change_function=revert_file_metadata,
            )

            # Revert persistent events
            await self.migrate_docs_in_collection(
                coll_name=IFRS_PERSISTED_EVENTS,
                change_function=revert_event,
            )
