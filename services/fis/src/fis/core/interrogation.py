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

from hexkit.utils import now_utc_ms_prec
from pydantic import UUID4, SecretBytes

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

    async def check_if_removable(self, file_id: UUID4) -> bool:
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

    async def handle_interrogation_report(self, report: models.InterrogationReport):
        """Handle an interrogation report and publish the appropriate event.

        If the report relays a success, then deposit the secret with EKSS and publish
        an InterrogationSuccess event. Otherwise, publish an InterrogationFailure event.
        In both cases, set `interrogated=True`, `state="interrogated"`, and
        `state_updated=now()` for the `FileUnderInterrogation` event. In the case of
        interrogation failure, also set `can_remove=True`.
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

        # Set the 'interrogated' flag, state, and timestamp
        file.interrogated = True
        file.state = STATES.INTERROGATED
        file.state_updated = now_utc_ms_prec()

        # See if this is a success report or a failure report
        if report.passed:
            # Deposit the secret with the EKSS
            secret_id = await self._deposit_secret(report.secret)  # type: ignore

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
            # Publish event
            await self._publisher.publish_interrogation_failed(
                file_id=report.file_id,
                storage_alias=report.storage_alias,
                interrogated_at=report.interrogated_at,
                reason=report.reason,  # type: ignore
            )

    async def _deposit_secret(self, secret: SecretBytes) -> str:
        """Deposit a secret with the EKSS and get a secret id in return"""
        # TODO: use thomas's creations
        raise NotImplementedError()

    async def process_file_upload(self, file: models.FileUnderInterrogation) -> None:
        """Process a newly received file upload.

        Make sure we don't already have this file. If we don't, then add it to the DB.
        If we do, see if this information is old or new. If old, ignore it.
        If new, update our copy.
        """
        if file.state == STATES.INBOX:
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
        if file.state in [STATES.FAILED, STATES.ARCHIVED]:
            file.can_remove = True
            await self._dao.update(file)
            log.info(
                "File %s arrived with state %s. Set can_remove to True.",
                file.id,
                file.state.value,
            )
