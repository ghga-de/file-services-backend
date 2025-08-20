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

"""A dummy DAO"""

from collections.abc import AsyncIterator, Mapping
from typing import Any, TypeVar
from unittest.mock import AsyncMock, Mock

from hexkit.custom_types import ID
from hexkit.protocols.dao import ResourceAlreadyExistsError, ResourceNotFoundError
from pydantic import BaseModel

from ucs.core.models import FileUpload, FileUploadBox, S3UploadDetails


class MockDaoBase:
    """Assigns sync mock to unused methods"""

    with_transaction = Mock()


class MockDaoPubBase(MockDaoBase):
    """Assigns async mock to unused methods"""

    publish_pending = republish = AsyncMock()


DTO = TypeVar("DTO", bound=BaseModel)


def get_dao[DTO: BaseModel](*, dto_model: type[DTO], id_field: str):  # noqa: C901
    """Produce a dummy DAO for the given DTO model and id field"""

    class DummyDao[DTO]:  # type: ignore
        """Dummy dao that stores data in memory"""

        resources: list
        with_transaction = Mock()

        def __init__(self):
            """Initialize the dummy dao publisher"""
            self.resources = []

        @property
        def latest(self) -> DTO:
            """Return the most recently inserted resource"""
            return self.resources[-1]

        async def get_by_id(self, id_: ID) -> DTO:
            """Get the box via ID."""
            for resource in self.resources:
                if id_ == getattr(resource, id_field):
                    return resource
            raise ResourceNotFoundError(id_=id_)

        async def find_one(self, *, mapping: Mapping[str, Any]):
            """Just here to satisfy protocol"""
            raise NotImplementedError()

        async def find_all(self, *, mapping: Mapping[str, Any]) -> AsyncIterator[DTO]:
            """Just here to satisfy protocol"""
            for resource in self.resources:
                if all([getattr(resource, k) == v for k, v in mapping.items()]):
                    yield resource  # type: ignore

        async def insert(self, dto: DTO) -> None:
            """Insert a resource"""
            dto_id = getattr(dto, id_field)
            for resource in self.resources:
                if getattr(resource, id_field) == dto_id:
                    raise ResourceAlreadyExistsError(id_=dto_id)
            self.resources.append(dto)

        async def update(self, dto: DTO) -> None:
            """Update a resource"""
            for i, resource in enumerate(self.resources):
                if getattr(resource, id_field) == getattr(dto, id_field):
                    self.resources[i] = dto
                    break
            else:
                raise ResourceNotFoundError(id_=getattr(dto, id_field))

        async def delete(self, id_: ID) -> None:
            """Delete a resource by ID"""
            for i, resource in enumerate(self.resources):
                if getattr(resource, id_field) == id_:
                    del self.resources[i]
                    break
            else:
                raise ResourceNotFoundError(id_=id_)

        async def upsert(self, dto: DTO) -> None:
            """Upsert a resource"""
            for i, resource in enumerate(self.resources):
                if getattr(resource, id_field) == getattr(dto, id_field):
                    self.resources[i] = dto
                    break
            else:
                self.resources.append(dto)

    return DummyDao


def get_outbox_dao[DTO: BaseModel](*, dto_model: type[DTO], id_field: str):
    """Produce a dummy outbox DAO for the given DTO model and id field"""
    DummyDao = get_dao(dto_model=dto_model, id_field=id_field)  # noqa: N806

    class DummyOutboxDao(DummyDao):  # type: ignore
        publish_pending = republish = AsyncMock()

    return DummyOutboxDao


DummyFileUploadBoxDao = get_outbox_dao(dto_model=FileUploadBox, id_field="id")
DummyFileUploadDao = get_outbox_dao(dto_model=FileUpload, id_field="id")
DummyS3UploadDetailsDao = get_dao(dto_model=S3UploadDetails, id_field="file_id")
