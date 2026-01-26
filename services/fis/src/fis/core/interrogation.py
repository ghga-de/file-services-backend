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

"""Core logic managing FileUnderInterrogation objects and interpreting
InterrogationReports.
"""

import logging
from contextlib import suppress

import httpx
from hexkit.utils import now_utc_ms_prec
from pydantic import UUID4

from fis.config import Config
from fis.core import models
from fis.ports.inbound.interrogation import InterrogationHandlerPort
from fis.ports.outbound.dao import (
    FileDao,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from fis.ports.outbound.event_pub import EventPubTranslatorPort

STATES = models.FileUploadState
log = logging.getLogger(__name__)


class InterrogationHandler(InterrogationHandlerPort):
    """This is the core class that manages `FileUnderInterrogation` and
    `InterrogationReport` objects.
    """

    def __init__(
        self,
        *,
        config: Config,
        file_dao: FileDao,
        event_publisher: EventPubTranslatorPort,
    ):
        self._config = config
        self._dao = file_dao
        self._publisher = event_publisher

    async def check_if_removable(self, *, file_id: UUID4) -> bool:
        """Return `True` if a file can be removed from the interrogation bucket and
        `False` otherwise.
        """
        try:
            file = await self._dao.get_by_id(file_id)
        except ResourceNotFoundError:
            # If not found, log a warning, but indicate the file is removable
            log.warning("Did not find a record of a file with ID %s.", file_id)
            return True
        return file.can_remove

    async def does_file_exist(self, *, file_id: UUID4) -> bool:
        """Return `True` if there is a `FileUnderInterrogation` with a matching ID and
        return `False` if not.
        """
        try:
            _ = await self._dao.get_by_id(file_id)
            return True
        except ResourceNotFoundError:
            return False

    async def handle_interrogation_report(self, *, report: models.InterrogationReport):
        """Handle an interrogation report and publish the appropriate event.

        If the report relays a success, then deposit the secret with EKSS and publish
        an InterrogationSuccess event. Otherwise, publish an InterrogationFailure event.
        In both cases, set `interrogated=True`, `state="interrogated"`, and
        `state_updated=now()` for the `FileUnderInterrogation` event. In the case of
        interrogation failure, also set `can_remove=True`.

        Raises:
        - FileNotFoundError if there's no file with the ID specified in the report.
        - SecretDepositionError if there's a problem depositing the secret with EKSS.
        """
        # First make sure we have this file
        try:
            file = await self._dao.get_by_id(report.file_id)
        except ResourceNotFoundError as err:
            log.error(
                "Can't process report because no file with ID %s exists.",
                report.file_id,
            )
            raise self.FileNotFoundError(file_id=report.file_id) from err

        # See if this is a success report or a failure report
        if report.passed:
            # Update file state
            file.state = "interrogated"

            # Deposit the secret with the EKSS
            url = f"{self._config.ekss_api_url}/secrets"
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    content=report.secret.get_secret_value(),  # type: ignore
                )  # type: ignore

            if response.status_code != 201:
                error = self.SecretDepositionError(file_id=report.file_id)
                log.error(error)
                raise error

            secret_id = response.json()

            # Publish event
            await self._publisher.publish_interrogation_success(
                file_id=report.file_id,
                secret_id=secret_id,
                storage_alias=report.storage_alias,
                interrogated_at=report.interrogated_at,
                encrypted_parts_md5=report.encrypted_parts_md5,  # type: ignore
                encrypted_parts_sha256=report.encrypted_parts_sha256,  # type: ignore
            )
        else:
            # Update file state
            file.state = "failed"
            file.can_remove = True

            # Publish event
            await self._publisher.publish_interrogation_failed(
                file_id=report.file_id,
                storage_alias=report.storage_alias,
                interrogated_at=report.interrogated_at,
                reason=report.reason,  # type: ignore
            )

        # Set the 'interrogated' flag and timestamp, and update the FileUnderInterrogation
        file.interrogated = True
        file.state_updated = now_utc_ms_prec()
        await self._dao.update(file)

    async def process_file_upload(self, *, file: models.FileUnderInterrogation) -> None:
        """Process a newly received file upload.

        Make sure we don't already have this file. If we don't, then add it to the DB.
        If we do, see if this information is old or new. If old, ignore it.
        If new, update our copy.
        """
        if file.state == "inbox":
            with suppress(ResourceAlreadyExistsError):
                await self._dao.insert(file)
                return

        # If we make it past that block, then it is not new and we must compare.
        local_file = await self._dao.get_by_id(file.id)

        # Ignore if outdated
        if local_file.state_updated >= file.state_updated:
            log.info("Encountered old data for file %s, ignoring.", file.id)
            return

        # If not outdated, see if the state is one we're interested in
        if file.state in ["failed", "archived"]:
            file.can_remove = True
            await self._dao.update(file)
            log.info(
                "File %s arrived with state %s. Set can_remove to True.",
                file.id,
                file.state,
            )

    async def get_files_not_yet_interrogated(
        self, *, data_hub: str
    ) -> list[models.BaseFileInformation]:
        """Return a list of not-yet-interrogated files for a Data Hub"""
        files = [
            models.BaseFileInformation(**x.model_dump())
            async for x in self._dao.find_all(mapping={"data_hub": data_hub})
        ]
        log.info("Fetched list of %i files for data hub %s.", len(files), data_hub)
        return files
