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

"""DAO translators for accessing the database."""

from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.protocols.dao import DaoFactoryProtocol

from dcs.adapters.outbound.utils import assert_record_is_new, make_record_from_update
from dcs.core import models
from dcs.core.models import FileDeletionRequestedRecord
from dcs.ports.inbound.data_repository import DataRepositoryPort
from dcs.ports.outbound.dao import (
    DrsObjectDaoPort,
    FileDeletionRequestedDaoPort,
    OutboxCoreInterfacePort,
)


async def get_drs_dao(*, dao_factory: DaoFactoryProtocol) -> DrsObjectDaoPort:
    """Setup the DAOs using the specified provider of the
    DaoFactoryProtocol.
    """
    return await dao_factory.get_dao(
        name="drs_objects",
        dto_model=models.AccessTimeDrsObject,
        id_field="file_id",
    )


async def get_file_deletion_requested_dao(
    *, dao_factory: DaoFactoryProtocol
) -> FileDeletionRequestedDaoPort:
    """Setup the DAOs using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name="file_deletion_requested",
        dto_model=FileDeletionRequestedRecord,
        id_field="file_id",
    )


class OutboxCoreInterface(OutboxCoreInterfacePort):
    """Class used to abstract idempotence away from the core for outbox events."""

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
