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

"""Main business-logic of this service"""

import logging
from contextlib import suppress

from ghga_service_commons.utils.multinode_storage import ObjectStorages
from hexkit.protocols.dao import NoHitsFoundError
from pydantic import UUID4

from ifrs.config import Config
from ifrs.constants import TRACER
from ifrs.core import models
from ifrs.ports.inbound.file_registry import FileRegistryPort
from ifrs.ports.outbound.dao import (
    FileAccessionDao,
    PendingFileDao,
    ResourceNotFoundError,
    file_dao,
)
from ifrs.ports.outbound.event_pub import EventPublisherPort

log = logging.getLogger(__name__)


class FileRegistry(FileRegistryPort):
    """A service that manages a registry files stored on a permanent object storage."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        file_metadata_dao: file_dao,
        pending_file_dao: PendingFileDao,
        file_accession_dao: FileAccessionDao,
        event_publisher: EventPublisherPort,
        object_storages: ObjectStorages,
        config: Config,
    ):
        """Initialize with essential config params and outbound adapters."""
        self._event_publisher = event_publisher
        self._file_metadata_dao = file_metadata_dao
        self._pending_file_dao = pending_file_dao
        self._file_accession_dao = file_accession_dao
        self._object_storages = object_storages
        self._config = config

    async def _is_file_registered(self, *, file: models.FileMetadata) -> bool:
        """Checks if the specified file is already registered. There are three possible
        outcomes:
            - Yes, the file has been registered with metadata that is identical to the
              provided one => returns `True`
            - Yes, however, the metadata differs => raises self.FileUpdateError
            - No, the file has not been registered, yet => returns `False`
        """
        try:
            registered_file = await self._file_metadata_dao.get_by_id(file.id)
        except ResourceNotFoundError:
            return False

        if file.model_dump() == registered_file.model_dump():
            return True

        raise self.FileUpdateError(file_id=file.id)

    @TRACER.start_as_current_span("FileRegistry.register_file")
    async def register_file(self, *, file: models.FileMetadata) -> None:
        """Registers a file and moves its content from the interrogation bucket into
        permanent storage. If the file with that exact metadata has already been
        registered, nothing is done.

        Raises:
            self.FileNotInInterrogationError:
                When the file content is not present in the interrogation bucket.
            self.SizeMismatchError:
                When the file size on the received metadata doesn't match the actual
                object size in the interrogation bucket.
            ValueError:
                When the configuration for the storage alias is not found.
            self.CopyOperationError:
                When an error occurs while attempting to copy the object to the
                permanent storage bucket.
        """
        storage_alias = file.storage_alias

        try:
            permanent_bucket_id, object_storage = self._object_storages.for_alias(
                storage_alias
            )
        except KeyError as error:
            alias_not_configured = ValueError(
                f"Storage alias '{storage_alias}' not configured."
            )
            log.critical(
                alias_not_configured,
                extra={"storage_alias": storage_alias},
                exc_info=True,
            )
            raise alias_not_configured from error

        try:
            if await self._is_file_registered(file=file):
                # There is nothing to do:
                log.info("File with ID '%s' is already registered.", file.id)
                return
        except self.FileUpdateError as error:
            # trying to re-register with different metadata should not crash the consumer
            # this is not a service internal inconsistency and would cause unnecessary
            # crashes on additional consumption attempts
            log.warning(error)
            return

        # Validate the file size against the expected value
        try:
            actual_size = await object_storage.get_object_size(
                bucket_id=permanent_bucket_id, object_id=str(file.id)
            )
            if actual_size != file.encrypted_size:
                raise self.SizeMismatchError(
                    file_id=file.id,
                    expected_size=file.encrypted_size,
                    actual_size=actual_size,
                )
        except object_storage.ObjectNotFoundError as exc:
            # The object does not exist in the interrogation bucket.
            content_not_in_staging = self.FileNotInInterrogationError(file_id=file.id)
            log.error(content_not_in_staging, extra={"file_id": file.id}, exc_info=True)
            raise content_not_in_staging from exc

        # Copy the file from interrogation to permanent storage
        try:
            await object_storage.copy_object(
                source_bucket_id=file.bucket_id,
                source_object_id=str(file.id),
                dest_bucket_id=permanent_bucket_id,
                dest_object_id=str(file.id),
            )
        except object_storage.ObjectAlreadyExistsError:
            # the content is already where it should go, there is nothing to do
            log.info(
                "Object corresponding to file ID '%s' is already in permanent storage.",
                file.id,
            )
            return
        except Exception as exc:
            # Irreconcilable object error -- event needs investigation
            obj_error = self.CopyOperationError(
                file_id=file.id,
                dest_bucket_id=permanent_bucket_id,
                exc_text=str(exc),
            )
            log.critical(obj_error, exc_info=True)
            raise obj_error from exc

        # Log the registration and publish an event
        log.info("Inserting file with file ID '%s'.", file.id)
        await self._file_metadata_dao.insert(file)

        await self._event_publisher.file_internally_registered(file=file)

    @TRACER.start_as_current_span("FileRegistry.stage_registered_file")
    async def stage_registered_file(
        self,
        *,
        accession: str,
        decrypted_sha256: str,
        download_object_id: UUID4,
        download_bucket_id: str,
    ) -> None:
        """Stage a registered file to the outbox.

        Args:
            accession:
                The accession number assigned to the file.
            decrypted_sha256:
                The checksum of the decrypted content. This is used to make sure that
                this service and the outside client are talking about the same file.
            download_object_id:
                The UUID4 S3 object ID for the download bucket.
            download_bucket_id:
                The S3 bucket ID for the download bucket.

        Raises:
            self.ChecksumMismatchError:
                When the provided checksum did not match the expectations.
            self.FileInRegistryButNotInStorageError:
                When encountering inconsistency between the registry (the database) and
                the permanent storage. This is an internal service error, which should
                not happen, and not the fault of the client.
            self.CopyOperationError:
                When an error occurs while attempting to copy the object to the download
                bucket.
        """
        # TODO: If we decide that object_id/file_id should always be the same across
        #  file services, then we would remove download_object_id. There is currently
        #  no mandate for this though.
        try:
            file = await self._file_metadata_dao.find_one(
                mapping={"accession": accession}
            )
        except NoHitsFoundError:
            file_not_registered_error = self.FileNotInRegistryError(accession=accession)
            log.error(file_not_registered_error, extra={"accession": accession})
            return

        if decrypted_sha256 != file.decrypted_sha256:
            checksum_error = self.ChecksumMismatchError(
                file_id=file.id,
                provided_checksum=decrypted_sha256,
                expected_checksum=file.decrypted_sha256,
            )
            log.error(
                checksum_error,
                extra={
                    "file_id": file.id,
                    "accession": accession,
                    "provided_checksum": decrypted_sha256,
                    "expected_checksum": file.decrypted_sha256,
                },
            )
            raise checksum_error

        permanent_bucket_id, object_storage = self._object_storages.for_alias(
            file.storage_alias
        )

        # Copy the file from permanent storage bucket to the outbox (download) bucket
        try:
            await object_storage.copy_object(
                source_bucket_id=permanent_bucket_id,
                source_object_id=str(file.id),
                dest_bucket_id=download_bucket_id,
                dest_object_id=str(download_object_id),
            )
        except object_storage.ObjectAlreadyExistsError:
            # the content is already where it should go, there is nothing to do
            log.info(
                "File with ID '%s' is already in the outbox.",
                file.id,
                extra={
                    "file_id": file.id,
                    "accession": accession,
                    "outbox_bucket_id": download_bucket_id,
                    "outbox_object_id": download_object_id,
                },
            )
            return
        except object_storage.ObjectNotFoundError as exc:
            # file does not exist in permanent storage
            # copy_object fetches the source object size, which checks for existence first
            not_in_storage_error = self.FileInRegistryButNotInStorageError(
                file_id=file.id
            )
            log.critical(
                msg=not_in_storage_error,
                extra={
                    "file_id": file.id,
                    "accession": accession,
                    "bucket_id": permanent_bucket_id,
                },
            )
            raise not_in_storage_error from exc
        except Exception as exc:
            # Irreconcilable object error -- event needs investigation
            obj_error = self.CopyOperationError(
                file_id=file.id, dest_bucket_id=download_bucket_id, exc_text=str(exc)
            )
            log.critical(
                obj_error,
                exc_info=True,
                extra={
                    "file_id": file.id,
                    "accession": accession,
                    "bucket_id": permanent_bucket_id,
                },
            )
            raise obj_error from exc

        log.info(
            "File with ID '%s' (accession %s) has been staged to the outbox with"
            + " the object ID '%s'.",
            file.id,
            accession,
            download_object_id,
            extra={
                "file_id": file.id,
                "accession": accession,
                "outbox_object_id": download_object_id,
            },
        )

    @TRACER.start_as_current_span("FileRegistry.delete_file")
    async def delete_file(self, *, accession: str) -> None:
        """Deletes a file from the permanent storage and the internal database.
        If no file with that accession exists, do nothing.

        Args:
            accession:
                The accession number of the file that needs to be deleted.
        """
        try:
            file = await self._file_metadata_dao.find_one(
                mapping={"accession": accession}
            )
        except ResourceNotFoundError:
            log.info(
                "File with accession '%s' was not found in the database. Deletion cancelled.",
                accession,
            )
            return

        # Get object ID and storage instance
        bucket_id, object_storage = self._object_storages.for_alias(file.storage_alias)

        # Try to remove file from S3
        with suppress(object_storage.ObjectNotFoundError):
            # If file does not exist anyways, we are done.
            await object_storage.delete_object(
                bucket_id=bucket_id, object_id=str(file.id)
            )

        # Try to remove file from database
        with suppress(ResourceNotFoundError):
            # If file does not exist anyways, we are done.
            await self._file_metadata_dao.delete(id_=file.id)

        log.info(
            "Finished object storage and metadata deletion for '%s'",
            accession,
            extra={"file_id": file.id, "accession": accession, "bucket_id": bucket_id},
        )
        await self._event_publisher.file_deleted(file_id=accession)

    async def store_accessions(self, *, accession_map: models.AccessionMap) -> None:
        """Handle an accession map by storing it in the database and, if possible,
        archiving files for which the corresponding File Upload data has already
        been received.
        """
        # Loop through the mapping
        for accession, file_id in accession_map.model_dump().items():
            # First check if there's a pending file upload which can be immediately archived
            try:
                pending_file = await self._pending_file_dao.get_by_id(file_id)
            except ResourceNotFoundError:
                # Not received yet, so instead we must store the accession
                file_accession = models.FileIdToAccession(
                    file_id=file_id, accession=accession
                )
                await self._file_accession_dao.upsert(file_accession)
            else:
                # We DO have a pending file, so now we can archive it
                # TODO: Log statements
                file = models.FileMetadata(
                    **pending_file.model_dump(), accession=accession
                )
                await self.register_file(file=file)

    async def handle_file_upload(
        self, *, pending_file: models.PendingFileUpload
    ) -> None:
        """Store a file upload which is set to the 'awaiting_archival' state.

        If a matching accession number is already stored in the database for this file,
        then archival will begin immediately. Otherwise, the file data will be stored
        until the accession number is received.
        """
        try:
            file_accession = await self._file_accession_dao.get_by_id(pending_file.id)
        except ResourceNotFoundError:
            # Accession not received yet, so store the file information for now
            await self._pending_file_dao.upsert(pending_file)
        else:
            # We DO have the file accession. Start archival.
            # TODO: log statements
            file = models.FileMetadata(
                **pending_file.model_dump(), accession=file_accession.accession
            )
            await self.register_file(file=file)
