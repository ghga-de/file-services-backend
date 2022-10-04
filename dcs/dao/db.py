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

"""Database DAO"""

from typing import Any, Optional

from ghga_service_chassis_lib.postgresql import (
    PostgresqlConfigBase,
    SyncPostgresqlConnector,
)
from ghga_service_chassis_lib.utils import DaoGenericBase
from sqlalchemy.future import select

from .. import models
from ..config import CONFIG
from . import db_models


class DrsObjectNotFoundError(RuntimeError):
    """Thrown when trying to access a DrsObject with a file ID that doesn't
    exist in the database."""

    def __init__(self, file_id: Optional[str]):
        message = (
            "The DRS Object"
            + (f" with file ID '{file_id}' " if file_id else "")
            + " does not exist in the database."
        )
        super().__init__(message)


class DrsObjectAlreadyExistsError(RuntimeError):
    """Thrown when trying to create a new DrsObject with an file ID that already
    exist in the database."""

    def __init__(self, file_id: Optional[str]):
        message = (
            "The DRS object"
            + (f" with file ID '{file_id}' " if file_id else "")
            + " already exist in the database."
        )
        super().__init__(message)


# Since this is just a DAO stub without implementation, following pylint error are
# expected:
# pylint: disable=unused-argument,no-self-use
class DatabaseDao(DaoGenericBase):
    """
    A DAO base class for interacting with the database.

    It might throw following exception to communicate selected error events:
        - DrsObjectNotFoundError
        - DrsObjectAlreadyExistsError
    """

    def get_drs_object(self, file_id: str) -> models.DrsObjectComplete:
        """Get DRS object from the database"""
        ...

    def register_drs_object(self, drs_object: models.DrsObjectBase) -> None:
        """Register a new DRS object to the database."""
        ...

    def update_drs_object(self, drs_object: models.DrsObjectBase) -> None:
        """Update information for a DRS object in the database."""
        ...

    def unregister_drs_object(self, file_id: str) -> None:
        """
        Unregister a new DRS object with the specified file ID from the database.
        """
        ...


class PostgresDatabase(DatabaseDao):
    """
    An implementation of the  DatabaseDao interface using a PostgreSQL backend.
    """

    def __init__(self, config: PostgresqlConfigBase = CONFIG):
        """initialze DAO implementation"""

        super().__init__(config)
        self._postgresql_connector = SyncPostgresqlConnector(config)

        # will be defined on __enter__:
        self._session_cm: Any = None
        self._session: Any = None

    def __enter__(self):
        """Setup database connection"""

        self._session_cm = self._postgresql_connector.transactional_session()
        self._session = self._session_cm.__enter__()  # pylint: disable=no-member
        return self

    def __exit__(self, error_type, error_value, error_traceback):
        """Teardown database connection"""
        # pylint: disable=no-member
        self._session_cm.__exit__(error_type, error_value, error_traceback)

    def _get_orm_drs_object(self, file_id: str) -> db_models.DrsObject:
        """Internal method to get the ORM representation of a drs object by specifying
        its file ID"""

        statement = select(db_models.DrsObject).filter_by(file_id=file_id)
        orm_drs_object = self._session.execute(statement).scalars().one_or_none()

        if orm_drs_object is None:
            raise DrsObjectNotFoundError(file_id=file_id)

        return orm_drs_object

    def get_drs_object(self, file_id: str) -> models.DrsObjectComplete:
        """Get DRS object from the database"""

        orm_drs_object = self._get_orm_drs_object(file_id=file_id)
        return models.DrsObjectComplete.from_orm(orm_drs_object)

    def register_drs_object(self, drs_object: models.DrsObjectBase) -> None:
        """Register a new DRS object to the database."""

        # check for collisions in the database:
        try:
            self._get_orm_drs_object(file_id=drs_object.file_id)
        except DrsObjectNotFoundError:
            # this is expected
            pass
        else:
            # this is a problem
            raise DrsObjectAlreadyExistsError(file_id=drs_object.file_id)

        drs_object_dict = {
            **drs_object.dict(),
        }
        orm_drs_object = db_models.DrsObject(**drs_object_dict)
        self._session.add(orm_drs_object)

    def update_drs_object(self, drs_object: models.DrsObjectBase) -> None:
        """
        Update information for a DRS object in the database the fields that could
        change eg. checksums, size and format
        """

        orm_drs_object = self._get_orm_drs_object(file_id=drs_object.file_id)

        # Modify all fields that could have changed due to encryption etc.
        orm_drs_object.md5_checksum = drs_object.md5_checksum
        orm_drs_object.size = drs_object.size
        orm_drs_object.format = drs_object.format

    def unregister_drs_object(self, file_id: str) -> None:
        """
        Unregister a new DRS object with the specified file ID from the database.
        """

        orm_drs_object = self._get_orm_drs_object(file_id=file_id)
        self._session.delete(orm_drs_object)
