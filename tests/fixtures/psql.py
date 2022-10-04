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

"""Fixtures for testing the PostgreSQL functionalities"""

from dataclasses import dataclass
from typing import Generator, List

import pytest
from ghga_service_chassis_lib.postgresql import PostgresqlConfigBase
from ghga_service_chassis_lib.postgresql_testing import config_from_psql_container
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from dcs import models
from dcs.dao import db_models
from dcs.dao.db import PostgresDatabase

from . import state

existing_file_infos: List[models.DrsObjectBase] = []
non_existing_file_infos: List[models.DrsObjectBase] = []

for file in state.FILES.values():
    if file.populate_db:
        existing_file_infos.append(file.file_info)
    else:
        non_existing_file_infos.append(file.file_info)


def populate_db(db_url: str, file_infos: List[models.DrsObjectBase]):
    """Create and populates the DB"""

    # setup database and tables:
    engine = create_engine(db_url)
    db_models.Base.metadata.create_all(engine)

    # populate with test data:
    session_factor = sessionmaker(engine)
    with session_factor() as session:
        for existing_file_info in file_infos:
            param_dict = {
                **existing_file_info.dict(),
            }
            orm_entry = db_models.DrsObject(**param_dict)
            session.add(orm_entry)
        session.commit()


@dataclass
class PsqlState:
    """Info yielded by the `psql_fixture` function"""

    config: PostgresqlConfigBase
    database: PostgresDatabase
    existing_file_infos: List[models.DrsObjectBase]
    non_existing_file_infos: List[models.DrsObjectBase]


@pytest.fixture
def psql_fixture() -> Generator[PsqlState, None, None]:
    """Pytest fixture for tests of the Prostgres DAO implementation."""

    with PostgresContainer() as postgres:
        config = config_from_psql_container(postgres)
        populate_db(config.db_url, file_infos=existing_file_infos)

        with PostgresDatabase(config) as database:
            yield PsqlState(
                config=config,
                database=database,
                existing_file_infos=existing_file_infos,
                non_existing_file_infos=non_existing_file_infos,
            )
