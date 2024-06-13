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
"""Contains logic to serve outbox event data to the core in an idempotent manner."""

import logging
from contextlib import suppress

from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.correlation import get_correlation_id
from hexkit.protocols.dao import DaoNaturalId, NoHitsFoundError
from hexkit.providers.mongodb import MongoDbDaoFactory
from pydantic import BaseModel

from dcs.adapters.outbound.dao import get_file_deletion_requested_dao
from dcs.config import Config
from dcs.core.models import FileDeletionRequestedRecord
from dcs.ports.inbound.data_repository import DataRepositoryPort
from dcs.ports.inbound.idempotent import IdempotenceHandlerPort
from dcs.ports.outbound.dao import FileDeletionRequestedDaoPort

log = logging.getLogger(__name__)

__all__ = [
    "IdempotenceHandler",
    "get_idempotence_handler",
    "make_record_from_update",
    "assert_record_is_new",
]


def make_record_from_update(update: BaseModel) -> FileDeletionRequestedRecord:
    """Get an FileDeletionRequestedRecord containing the update payload and correlation ID."""
    correlation_id = get_correlation_id()
    return FileDeletionRequestedRecord(
        correlation_id=correlation_id, **update.model_dump()
    )


async def assert_record_is_new(
    dao: DaoNaturalId,
    resource_id: str,
    update: BaseModel,
    record: FileDeletionRequestedRecord,
):
    """Returns whether or not the record is new and emits a debug log if it is not."""
    with suppress(NoHitsFoundError):
        matching_record = await dao.find_one(mapping=record.model_dump())

        if matching_record:
            log.debug(
                (
                    "Event with '%s' schema for resource ID '%s' has"
                    + " already been processed under current correlation_id. Skipping."
                ),
                type(update).__name__,
                resource_id,
            )
            return False
    return True


async def get_idempotence_handler(
    *, config: Config, data_repository: DataRepositoryPort
) -> IdempotenceHandlerPort:
    """Get an instance of the IdempotenceHandler."""
    dao_factory = MongoDbDaoFactory(config=config)
    file_deletion_dao = await get_file_deletion_requested_dao(dao_factory=dao_factory)

    return IdempotenceHandler(
        file_deletion_dao=file_deletion_dao, data_repository=data_repository
    )


class IdempotenceHandler(IdempotenceHandlerPort):
    """Class to serve outbox event data to the core in an idempotent manner."""

    def __init__(
        self,
        *,
        file_deletion_dao: FileDeletionRequestedDaoPort,
        data_repository: DataRepositoryPort,
    ):
        """Initialize with config parameters and core dependencies."""
        self._data_repository = data_repository
        self._file_deletion_dao = file_deletion_dao

    async def upsert_file_deletion_requested(
        self, *, resource_id: str, update: event_schemas.FileDeletionRequested
    ) -> None:
        """Upsert a FileDeletionRequested event. Call `DataRepository.delete_file` if
        the idempotence check is passed.
        Args:
            resource_id:
                The resource ID.
            update:
                The FileDeletionRequested event to upsert.
        """
        record = make_record_from_update(update)
        if await assert_record_is_new(
            dao=self._file_deletion_dao,
            resource_id=resource_id,
            update=update,
            record=record,
        ):
            await self._data_repository.delete_file(file_id=resource_id)
            await self._file_deletion_dao.insert(record)
