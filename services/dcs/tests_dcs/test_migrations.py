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

"""Tests for DCS database migrations"""

from asyncio import sleep
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.providers.mongodb.testutils import MongoDbFixture

from dcs.core.models import AccessTimeDrsObject
from dcs.migrations import run_db_migrations
from tests_dcs.fixtures.config import get_config

pytestmark = pytest.mark.asyncio()


async def test_migration_v2(mongodb: MongoDbFixture):
    """Test the migration to DB version 2 and reversion to DB version 1."""
    config = get_config(sources=[mongodb.config])

    # Generate sample 'old' data that needs to be migrated
    data: list[dict[str, Any]] = []

    for i in range(3):
        old_drs_object = AccessTimeDrsObject(
            file_id=f"GHGAFile{i}",
            decryption_secret_id="abc123",
            decrypted_sha256="some-stuff",
            decrypted_size=100,
            encrypted_size=128,
            creation_date=now_as_utc(),
            s3_endpoint_alias="HD01",
            object_id=uuid4(),
            last_accessed=now_as_utc(),
        ).model_dump()

        # Convert data to the old format
        old_drs_object["_id"] = old_drs_object.pop("file_id")
        old_drs_object["object_id"] = str(old_drs_object["object_id"])
        old_drs_object["creation_date"] = old_drs_object["creation_date"].isoformat()
        old_drs_object["last_accessed"] = old_drs_object["last_accessed"].isoformat()
        data.append(old_drs_object)
        await sleep(0.1)  # sleep so timestamps are meaningfully different

    # Clear out anything so we definitely start with an empty collection
    db = mongodb.client[config.db_name]
    collection = db["drs_objects"]
    collection.delete_many({})

    # Insert the test data
    collection.insert_many(data)

    # Run the migration
    await run_db_migrations(config=config, target_version=2)

    # Retrieve the migrated data and compare
    migrated_data = collection.find().to_list()
    migrated_data.sort(key=lambda x: x["_id"])

    assert len(migrated_data) == len(data)
    for old, new in zip(data, migrated_data, strict=True):
        assert new["_id"] == old["_id"]

        new_creation = new["creation_date"]
        new_accessed = new["last_accessed"]

        # Make sure the migrated data has the right types
        assert isinstance(new["object_id"], UUID)
        assert isinstance(new_creation, datetime)
        assert isinstance(new_accessed, datetime)

        # Make sure the actual ID of the object ID field still matches the old one
        assert str(new["object_id"]) == old["object_id"]

        # rather than calculating exact date mig results (tested in hexkit), just verify
        #  that it's within half a millisecond
        max_time_diff = timedelta(microseconds=500)
        assert (
            abs(new_creation - datetime.fromisoformat(old["creation_date"]))
            < max_time_diff
        )
        assert (
            abs(new_accessed - datetime.fromisoformat(old["last_accessed"]))
            < max_time_diff
        )

        assert new_creation.microsecond % 1000 == 0
        assert new_accessed.microsecond % 1000 == 0

    # now unapply (dates will not have microseconds of course)
    await run_db_migrations(config=config, target_version=1)
    reverted_data = collection.find().to_list()
    reverted_data.sort(key=lambda x: x["_id"])
    assert len(reverted_data) == len(data)
    for reverted, new in zip(reverted_data, migrated_data, strict=True):
        assert isinstance(reverted["object_id"], str)
        assert isinstance(reverted["creation_date"], str)
        assert isinstance(reverted["last_accessed"], str)

        assert reverted["_id"] == str(new["_id"])
        assert reverted["creation_date"] == new["creation_date"].isoformat()
        assert reverted["last_accessed"] == new["last_accessed"].isoformat()
