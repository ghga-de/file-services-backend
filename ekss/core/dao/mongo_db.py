# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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
"""
Provides DAOs for insertion/retrieval to/from MongoDB and wrappers around this functionality
"""

import base64
from typing import Type

from hexkit.protocols.dao import DaoSurrogateId
from hexkit.providers.mongodb import MongoDbConfig, MongoDbDaoFactory

from ekss.core.dao.dto_models import FileSecretCreationDto, FileSecretDto


class FileSecretDao:
    """abstractions over file secret/GHGA secret DAOs"""

    def __init__(self, config: MongoDbConfig):
        self.config = config

    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: Type[FileSecretDto],
        dto_creation_model: Type[FileSecretCreationDto],
    ) -> DaoSurrogateId:
        """Get a DAO for either file or GHGA secrets"""
        dao_factory = MongoDbDaoFactory(config=self.config)
        return await dao_factory.get_dao(
            name=name,
            dto_model=dto_model,
            dto_creation_model=dto_creation_model,
            id_field="id",
        )

    async def _get_file_secret_dao(self) -> DaoSurrogateId:
        """Instantiate a DAO for file secret interactions"""
        return await self._get_dao(
            name="file_secrets",
            dto_model=FileSecretDto,
            dto_creation_model=FileSecretCreationDto,
        )

    async def get_file_secret(self, *, id_: str) -> bytes:
        """Retrieve file secret from db"""
        dao = await self._get_file_secret_dao()
        response = await dao.get_by_id(id_=id_)
        return base64.b64decode(response.file_secret)

    async def insert_file_secret(self, *, file_secret: bytes) -> FileSecretDto:
        """Encode and insert file secret into db"""
        secret = base64.b64encode(file_secret).decode("utf-8")
        file_secret_dto = FileSecretCreationDto(file_secret=secret)
        dao = await self._get_file_secret_dao()
        response = await dao.insert(file_secret_dto)
        return response
