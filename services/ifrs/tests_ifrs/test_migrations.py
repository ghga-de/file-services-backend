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
"""Tests for database migrations"""

from random import randint

import pytest
from ghga_service_commons.utils.multinode_storage import (
    S3ObjectStorageNodeConfig,
    S3ObjectStoragesConfig,
)
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.s3.testutils import S3Fixture, temp_file_object

from ifrs.core.models import FileMetadataBase
from ifrs.migration_logic._manager import MigrationStepError
from ifrs.migrations import run_db_migrations
from tests_ifrs.fixtures.config import get_config
from tests_ifrs.fixtures.example_data import EXAMPLE_METADATA_BASE


@pytest.mark.asyncio
async def test_v2_migration(
    mongodb: MongoDbFixture,
    s3: S3Fixture,
    populate_s3_buckets,
    monkeypatch: pytest.MonkeyPatch,
):
    """Test the migration script for v2 where we add the size of the encrypted object
    to the docs in the file_metadata collection.
    """
    # Create the test config for the object storage
    node_config = S3ObjectStorageNodeConfig(bucket="permanent", credentials=s3.config)
    object_storage_config = S3ObjectStoragesConfig(
        object_storages={
            "test": node_config,
        }
    )

    # Patch the config
    config = get_config(sources=[mongodb.config, object_storage_config])
    monkeypatch.setattr("ifrs.migration_logic.ifrs_migrations.Config", lambda: config)
    client = mongodb.client
    coll = client[config.db_name]["file_metadata"]

    # Set up test data - we want some random object sizes to verify we're not accidentally
    #  reading some dummy value somewhere. Record the sizes so we can check later that
    #  what we retrieve is correct.
    test_data = []
    sizes = []
    base = EXAMPLE_METADATA_BASE.model_dump()
    base.pop("file_id")
    for n in range(1, 10):
        metadata_base = base.copy()
        metadata_base["_id"] = f"examplefile00{n}"
        metadata_base["object_id"] = f"objectid00{n}"
        test_data.append(metadata_base)
        size = randint(1000, 4000)
        sizes.append(size)
        coll.insert_one(metadata_base)

        # Upload a temp file object with the given size to our S3 fixture
        with temp_file_object(
            bucket_id="permanent", object_id=f"objectid00{n}", size=size
        ) as f:
            await s3.populate_file_objects([f])

    post_insert = [_ for _ in coll.find()]
    assert len(post_insert) == 9
    for doc in post_insert:
        assert "object_size" not in doc

    # Run the database migrations, which should create the lock/versioning collection
    #  and then update the 'file_metadata' collection.
    await run_db_migrations(config=config)

    # Make sure the docs from file_metadata were updated with the right size values
    post_apply = [_ for _ in coll.find()]
    assert len(post_apply) == 9
    for doc, size in zip(post_apply, sizes, strict=True):
        assert "object_size" in doc
        assert doc["object_size"] == size

    # Unapply the v2 migration, which will fail because the current model requires
    #  the `object_size` field.
    with pytest.raises(MigrationStepError):
        await run_db_migrations(config=config, target_version=1)

    # Make sure the docs from file_metadata were updated with the right size values
    post_failed_unapply = [_ for _ in coll.find()]
    assert len(post_failed_unapply) == 9
    for doc, size in zip(post_failed_unapply, sizes, strict=True):
        assert "object_size" in doc
        assert doc["object_size"] == size

    # Monkeypatch the pydantic model to resemble the old version so we can test reversal
    class PatchedFileMetadata(FileMetadataBase):
        object_id: str

    monkeypatch.setattr(
        "ifrs.migration_logic.ifrs_migrations.FileMetadata", PatchedFileMetadata
    )
    await run_db_migrations(config=config, target_version=1)

    post_unapply = [_ for _ in coll.find()]
    assert len(post_unapply) == 9
    for doc in post_unapply:
        assert "object_size" not in doc
