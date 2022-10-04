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

"""Tests the database DAO implementation base on PostgreSQL"""

import pytest

from dcs.dao.db import DrsObjectAlreadyExistsError, DrsObjectNotFoundError

from ..fixtures import psql_fixture  # noqa: F401


def test_get_existing_file_obj(psql_fixture):  # noqa: F811
    """Test getting exiting file object."""

    existing_file_obj = psql_fixture.existing_file_infos[0]

    returned_file_obj = psql_fixture.database.get_drs_object(existing_file_obj.file_id)

    assert existing_file_obj.md5_checksum == returned_file_obj.md5_checksum


def test_get_non_existing_file_obj(psql_fixture):  # noqa: F811
    """Test getting not existing file object and expect corresponding error."""

    non_existing_file_obj = psql_fixture.non_existing_file_infos[0]

    with pytest.raises(DrsObjectNotFoundError):
        psql_fixture.database.get_drs_object(non_existing_file_obj.file_id)


def test_register_non_existing_file_obj(psql_fixture):  # noqa: F811
    """Test registering not existing file object."""

    non_existing_file_obj = psql_fixture.non_existing_file_infos[0]

    psql_fixture.database.register_drs_object(non_existing_file_obj)
    returned_file_obj = psql_fixture.database.get_drs_object(
        non_existing_file_obj.file_id
    )

    assert non_existing_file_obj.md5_checksum == returned_file_obj.md5_checksum


def test_register_existing_file_obj(psql_fixture):  # noqa: F811
    """Test registering an already existing file object and expect corresponding
    error."""

    existing_file_obj = psql_fixture.existing_file_infos[0]

    with pytest.raises(DrsObjectAlreadyExistsError):
        psql_fixture.database.register_drs_object(existing_file_obj)


def test_unregister_non_existing_file_obj(psql_fixture):  # noqa: F811
    """Test unregistering not existing file object and expect corresponding error."""

    non_existing_file_obj = psql_fixture.non_existing_file_infos[0]

    with pytest.raises(DrsObjectNotFoundError):
        psql_fixture.database.unregister_drs_object(non_existing_file_obj.file_id)


def test_unregister_existing_file_obj(psql_fixture):  # noqa: F811
    """Test unregistering an existing file object."""

    existing_file_obj = psql_fixture.existing_file_infos[0]

    psql_fixture.database.unregister_drs_object(existing_file_obj.file_id)

    # check if file object can no longer be found:
    with pytest.raises(DrsObjectNotFoundError):
        psql_fixture.database.get_drs_object(existing_file_obj.file_id)
