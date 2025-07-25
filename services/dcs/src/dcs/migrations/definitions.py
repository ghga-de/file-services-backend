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

"""Database migration logic for DCS"""

from hexkit.providers.mongodb.migrations import (
    Document,
    MigrationDefinition,
    Reversible,
)
from hexkit.providers.mongodb.migrations.helpers import convert_uuids_and_datetimes_v6

from dcs.core.models import AccessTimeDrsObject

DRS_OBJECTS = "drs_objects"


class V2Migration(MigrationDefinition, Reversible):
    """Update the stored data to have native-typed UUIDs and datetimes.

    This impacts the object_id, creation_date, and last_accessed fields on the
    AccessTimeDrsObject model.

    This can be reversed by converting the UUIDs and datetimes back to strings.
    """

    version = 2

    _uuid_field: str = "object_id"
    _date_fields: list[str] = ["creation_date", "last_accessed"]

    async def apply(self):
        """Perform the migration."""
        convert_drs_objects = convert_uuids_and_datetimes_v6(
            uuid_fields=[self._uuid_field], date_fields=self._date_fields
        )

        async with self.auto_finalize(coll_names=DRS_OBJECTS, copy_indexes=True):
            await self.migrate_docs_in_collection(
                coll_name=DRS_OBJECTS,
                change_function=convert_drs_objects,
                validation_model=AccessTimeDrsObject,
                id_field="file_id",
            )

    async def unapply(self):
        """Revert the migration."""

        # define the change function
        async def revert_drs_objects(doc: Document) -> Document:
            """Convert the fields back into strings"""
            doc[self._uuid_field] = str(doc[self._uuid_field])
            for field in self._date_fields:
                doc[field] = doc[field].isoformat()
            return doc

        async with self.auto_finalize(coll_names=DRS_OBJECTS, copy_indexes=True):
            # Don't provide validation models here
            await self.migrate_docs_in_collection(
                coll_name=DRS_OBJECTS,
                change_function=revert_drs_objects,
            )
