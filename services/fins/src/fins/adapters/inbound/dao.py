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
"""DAO translators for database access."""

from hexkit.protocols.dao import DaoFactoryProtocol

from fins.core import models
from fins.ports.inbound.dao import FileDeletionRequestedDaoPort, FileInformationDaoPort


async def get_file_deletion_requested_dao(
    *, dao_factory: DaoFactoryProtocol
) -> FileDeletionRequestedDaoPort:
    """Setup the DAOs using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name="file_deletion_requested",
        dto_model=models.FileDeletionRequested,
        id_field="file_id",
    )


async def get_file_information_dao(
    *, dao_factory: DaoFactoryProtocol
) -> FileInformationDaoPort:
    """Setup the DAOs using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name="file_information",
        dto_model=models.FileInformation,
        id_field="file_id",
    )
