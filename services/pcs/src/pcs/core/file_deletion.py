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

"""Main business-logic of this service"""

from ghga_event_schemas.pydantic_ import FileDeletionRequested

from pcs.ports.inbound.file_deletion import FileDeletionPort
from pcs.ports.outbound.daopub import FileDeletionDao


class FileDeletion(FileDeletionPort):
    """A service that commissions file deletions."""

    def __init__(self, *, file_deletion_dao: FileDeletionDao):
        """Initialize with outbound adapters."""
        self._file_deletion_dao = file_deletion_dao

    async def delete_file(self, *, file_id: str) -> None:
        """Sends out an event to delete all occurrences of a certain file.

        Args:
            file_id: id for the file to delete.
        """
        file_deletion = FileDeletionRequested(file_id=file_id)
        await self._file_deletion_dao.upsert(file_deletion)
