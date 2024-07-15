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
"""TODO"""

import logging

import ghga_event_schemas.pydantic_ as event_schemas
from hexkit.protocols.dao import ResourceAlreadyExistsError, ResourceNotFoundError

from fins.adapters.inbound.dao import FileInformationDaoPort
from fins.core.models import FileInformation
from fins.ports.inbound.information_service import InformationServicePort

log = logging.getLogger(__name__)


class InformationService(InformationServicePort):
    """A service that handles storage and deletion of relevant metadata for files
    registered with the Internal File Registry service.
    """

    def __init__(self, *, file_infomation_dao: FileInformationDaoPort):
        self._file_information_dao = file_infomation_dao

    async def deletion_requested(self, file_id: str):
        """Handle deletion requestes for information associated with the give file ID."""
        try:
            await self._file_information_dao.get_by_id(id_=file_id)
        except ResourceNotFoundError:
            log.info(
                f"Information for file with id '{file_id}' has already been deleted."
            )
            return

        await self._file_information_dao.delete(id_=file_id)
        log.info(f"Successfully deleted entries for file with id '{file_id}'.")

    async def register_information(
        self, file_registered: event_schemas.FileInternallyRegistered
    ):
        """Store information for a file newly registered with the Internal File Registry."""
        file_information = FileInformation(
            file_id=file_registered.file_id,
            size=file_registered.decrypted_size,
            sha256_hash=file_registered.decrypted_sha256,
        )

        try:
            await self._file_information_dao.insert(file_information)
        except ResourceAlreadyExistsError:
            information_exists = self.InformationAlreadyRegistered(
                file_id=file_information.file_id
            )
            log.warning(information_exists)

    async def serve_information(self, file_id: str) -> FileInformation:
        """Retrieve stored public information for the five file ID."""
        try:
            file_information = await self._file_information_dao.get_by_id(file_id)
        except ResourceNotFoundError as error:
            information_not_found = self.InformationNotFoundError(file_id=file_id)
            log.warning(information_not_found)
            raise information_not_found from error

        return file_information
