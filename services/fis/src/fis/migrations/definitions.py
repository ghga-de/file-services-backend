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

"""Database migration logic for FIS"""

from hexkit.providers.mongodb.migrations import (
    MigrationDefinition,
    Reversible,
    validate_doc,
)
from hexkit.providers.mongokafka.provider.persistent_pub import PersistentKafkaEvent

from fis.config import Config


class V2Migration(MigrationDefinition, Reversible):
    """Move stored outbox events from the `file-validations` to two new collections:

    1. `ingestedFiles`: where each document merely contains the file ID
    2. `fisPersistedEvents`: a collection used for event persistence (replaces outbox)
    """

    version = 2

    async def apply(self):
        """Perform the migration."""
        file_validations_collection = self._db["file-validations"]
        ingested_files_collection = self._db["ingestedFiles"]
        persisted_events_collection = self._db["fisPersistedEvents"]
        config = Config()

        topic = config.file_interrogations_topic
        type_ = config.interrogation_success_type

        # Get the file ID and metadata from the outbox events
        async for doc in file_validations_collection.find():
            file_id = doc.pop("_id")
            outbox_metadata = doc.pop("__metadata__", {})
            persistent_event = {
                "_id": f"{topic}:{file_id}",
                "topic": topic,
                "type_": type_,
                "payload": doc,
                "key": file_id,
                "headers": {},
                "correlation_id": outbox_metadata.get("correlation_id"),
                "created": doc["upload_date"],
                "published": outbox_metadata.get("published"),
            }

            # Validate the persistent event
            validate_doc(persistent_event, model=PersistentKafkaEvent, id_field="id")

            # Insert records into the v2 collections
            await ingested_files_collection.insert_one({"_id": file_id})
            await persisted_events_collection.insert_one(persistent_event)

        # Drop the old outbox events collection
        await file_validations_collection.drop()

    async def unapply(self):
        """Unapply the migration"""
        ingested_files_collection = self._db["ingestedFiles"]
        persisted_events_collection = self._db["fisPersistedEvents"]

        # Get old collection name from config
        config = Config()
        collection_name = getattr(config, "file_validations_collection")  # noqa: B009
        file_validations_collection = self._db[collection_name]

        # Convert persisted events back into the outbox event format with __metadata__
        async for doc in persisted_events_collection.find():
            file_id = doc.pop("_id")
            doc_to_insert = doc.pop("payload")
            doc_to_insert["_id"] = file_id
            doc_to_insert["__metadata__"] = {
                "deleted": False,
                "published": doc.pop("published"),
                "correlation_id": doc.pop("correlation_id"),
            }
            await file_validations_collection.insert_one(doc_to_insert)

        # Drop both v2 collections
        await ingested_files_collection.drop()
        await persisted_events_collection.drop()
