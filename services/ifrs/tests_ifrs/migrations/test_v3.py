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

"""Tests for V3 migration logic"""

from uuid import uuid4

import pytest
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.utils import now_utc_ms_prec
from tests_ifrs.fixtures.config import get_config

from ifrs.migrations import run_db_migrations

pytestmark = pytest.mark.asyncio


async def test_v3_migration(mongodb: MongoDbFixture):
    """Test the v3 migration which renames and reorganizes fields in file_metadata
    and ifrsPersistedEvents collections for the Sarcastic Fringehead epic.
    """
    config = get_config(sources=[mongodb.config])
    db = mongodb.client[config.db_name]
    events_collection = db["ifrsPersistedEvents"]
    metadata_collection = db["file_metadata"]

    # Create test data for the FileMetadata objects
    old_metadata = [
        {
            "_id": f"GHGA00{i}",
            "upload_date": now_utc_ms_prec(),
            "decryption_secret_id": f"secret-{i}",
            "content_offset": 0,
            "encrypted_part_size": 16 * 1024**2,
            "decrypted_size": 64 * 1024**2,
            "encrypted_parts_md5": ["md5-1", "md5-2"],
            "encrypted_parts_sha256": ["sha-1", "sha-2"],
            "decrypted_sha256": "decrypted-checksum",
            "storage_alias": "test-storage",
            "object_id": uuid4(),
            "object_size": 64 * 1024**2 + 1000,
            "bucket_id": "test-bucket",
        }
        for i in range(3)
    ]

    expected_migrated_metadata = []
    for old_doc in old_metadata:
        migrated = {
            "accession": old_doc["_id"],
            "_id": old_doc["object_id"],
            "archive_date": old_doc["upload_date"],
            "storage_alias": old_doc["storage_alias"],
            "bucket_id": old_doc["bucket_id"],
            "secret_id": old_doc["decryption_secret_id"],
            "decrypted_size": old_doc["decrypted_size"],
            "encrypted_size": old_doc["object_size"],
            "decrypted_sha256": old_doc["decrypted_sha256"],
            "encrypted_parts_md5": old_doc["encrypted_parts_md5"],
            "encrypted_parts_sha256": old_doc["encrypted_parts_sha256"],
            "part_size": old_doc["encrypted_part_size"],
        }
        expected_migrated_metadata.append(migrated)

    expected_reverted_metadata = []
    for old_doc in old_metadata:
        reverted = old_doc.copy()
        reverted["content_offset"] = 0
        expected_reverted_metadata.append(reverted)

    # Create persisted event data for the FileInternallyRegistered events
    affected_payload = {
        "file_id": "GHGA001",
        "object_id": uuid4(),
        "upload_date": now_utc_ms_prec(),
        "s3_endpoint_alias": "HD01",
        "decryption_secret_id": "test-secret-id",
        "encrypted_part_size": 16 * 1024**2,
        "content_offset": 128,
        "bucket_id": "test-bucket",
        "decrypted_size": 64 * 1024**2,
        "encrypted_size": 64 * 1024**2 + 1000,
        "decrypted_sha256": "checksum",
        "encrypted_parts_md5": ["md5"],
        "encrypted_parts_sha256": ["sha"],
    }

    unaffected_payload = {"some_field": "some_value"}

    old_events = [
        {
            "_id": "test-topic:key0",
            "topic": "test-topic",
            "payload": affected_payload,
            "key": "key0",
            "type_": "file_internally_registered",
            "headers": {},
            "correlation_id": uuid4(),
            "created": now_utc_ms_prec(),
            "published": True,
            "event_id": uuid4(),
        },
        {
            "_id": "test-topic:key1",
            "topic": "test-topic",
            "payload": unaffected_payload,
            "key": "key1",
            "type_": "other_event_type",
            "headers": {},
            "correlation_id": uuid4(),
            "created": now_utc_ms_prec(),
            "published": True,
            "event_id": uuid4(),
        },
    ]
    expected_migrated_payload = {
        "accession": "GHGA001",
        "file_id": affected_payload["object_id"],
        "archive_date": affected_payload["upload_date"],
        "storage_alias": "HD01",
        "secret_id": affected_payload["decryption_secret_id"],
        "part_size": 16 * 1024**2,
        "bucket_id": "test-bucket",
        "decrypted_size": 64 * 1024**2,
        "encrypted_size": 64 * 1024**2 + 1000,
        "decrypted_sha256": "checksum",
        "encrypted_parts_md5": ["md5"],
        "encrypted_parts_sha256": ["sha"],
    }

    # Clear collections and insert old data
    events_collection.delete_many({})
    metadata_collection.delete_many({})
    events_collection.insert_many(old_events)
    metadata_collection.insert_many(old_metadata)

    # Run the migration (at last)
    await run_db_migrations(config=config, target_version=3)

    # Verify file metadata was migrated properly
    migrated_metadata = sorted(
        metadata_collection.find().to_list(), key=lambda d: d["accession"]
    )
    assert len(migrated_metadata) == len(expected_migrated_metadata)
    assert migrated_metadata == expected_migrated_metadata

    # Verify that the events were migrated
    migrated_events = sorted(
        events_collection.find({}).to_list(), key=lambda d: d["_id"]
    )
    assert len(migrated_events) == 2
    assert migrated_events[0]["payload"] == expected_migrated_payload
    assert migrated_events[0]["published"] == False
    # Second event not touched
    assert migrated_events[1]["payload"] == unaffected_payload
    assert migrated_events[1]["published"] == True

    # Run the reverse migration
    await run_db_migrations(config=config, target_version=2)

    # Verify FileMetadata reversion
    reverted_metadata = sorted(
        metadata_collection.find({}).to_list(), key=lambda d: d["_id"]
    )
    assert len(reverted_metadata) == len(expected_reverted_metadata)
    assert reverted_metadata == expected_reverted_metadata

    # Verify events reversion
    reverted_events = sorted(events_collection.find().to_list(), key=lambda d: d["_id"])
    reverted_payload = reverted_events[0]["payload"]
    assert reverted_payload["content_offset"] == 0
    assert reverted_payload["file_id"] == affected_payload["file_id"]
    assert reverted_payload["object_id"] == affected_payload["object_id"]
    assert "accession" not in reverted_payload


async def test_v3_migration_on_already_migrated_file_metadata(mongodb: MongoDbFixture):
    """Verify that V3 migration gracefully handles already migrated file_metadata
    by skipping it (checking for presence of 'accession' field).
    """
    config = get_config(sources=[mongodb.config])
    db = mongodb.client[config.db_name]
    metadata_collection = db["file_metadata"]

    metadata_collection.delete_many({})
    await run_db_migrations(config=config, target_version=2)
    migrated_data = {
        "_id": uuid4(),
        "accession": "GHGA001",
        "archive_date": now_utc_ms_prec(),
        "secret_id": "test-secret",
        "part_size": 16 * 1024**2,
        "encrypted_size": 64 * 1024**2 + 1000,
        "decrypted_size": 64 * 1024**2,
        "encrypted_parts_md5": ["md5"],
        "encrypted_parts_sha256": ["sha"],
        "decrypted_sha256": "checksum",
        "storage_alias": "test-storage",
        "bucket_id": "test-bucket",
    }

    metadata_collection.insert_one(migrated_data)
    await run_db_migrations(config=config, target_version=3)
    result = metadata_collection.find_one({"accession": "GHGA001"})
    assert result is not None
    assert result["accession"] == "GHGA001"
    assert "content_offset" not in result


async def test_v3_migration_on_already_migrated_events(mongodb: MongoDbFixture):
    """Verify that V3 migration gracefully handles already migrated events
    by skipping them (checking for absence of 'content_offset' in payload).
    """
    config = get_config(sources=[mongodb.config])
    db = mongodb.client[config.db_name]
    events_collection = db["ifrsPersistedEvents"]

    events_collection.delete_many({})
    await run_db_migrations(config=config, target_version=2)
    migrated_event = {
        "_id": "test-topic:test-key",
        "topic": "test-topic",
        "payload": {
            "accession": "GHGA:file001",
            "file_id": uuid4(),
            "archive_date": now_utc_ms_prec(),
            "storage_alias": "HD01",
            "secret_id": "test-secret-id",
            "part_size": 16 * 1024**2,
            "bucket_id": "test-bucket",
            "decrypted_size": 64 * 1024**2,
            "encrypted_size": 64 * 1024**2 + 1000,
            "decrypted_sha256": "checksum",
            "encrypted_parts_md5": ["md5"],
            "encrypted_parts_sha256": ["sha"],
        },
        "key": "test-key",
        "type_": "file_internally_registered",
        "headers": {},
        "correlation_id": uuid4(),
        "created": now_utc_ms_prec(),
        "published": False,
        "event_id": uuid4(),
    }

    events_collection.insert_one(migrated_event)
    await run_db_migrations(config=config, target_version=3)
    result = events_collection.find_one({"_id": "test-topic:test-key"})
    assert result is not None
    assert "content_offset" not in result["payload"]
    assert result["payload"]["accession"] == "GHGA:file001"


async def test_v3_migration_skips_non_file_internally_registered_events(
    mongodb: MongoDbFixture,
):
    """Verify that V3 migration only affects FileInternallyRegistered events
    and leaves other event types untouched.
    """
    config = get_config(sources=[mongodb.config])
    db = mongodb.client[config.db_name]
    events_collection = db["ifrsPersistedEvents"]

    events_collection.delete_many({})
    await run_db_migrations(config=config, target_version=2)
    unaffected_events = [
        {
            "_id": "test-topic:key1",
            "topic": "test-topic",
            "payload": {"file_id": "some-id", "status": "completed"},
            "key": "key1",
            "type_": "file_deleted",
            "headers": {},
            "correlation_id": uuid4(),
            "created": now_utc_ms_prec(),
            "published": True,
            "event_id": uuid4(),
        },
        {
            "_id": "test-topic:key2",
            "topic": "test-topic",
            "payload": {"user_id": "user123", "action": "login"},
            "key": "key2",
            "type_": "user_action",
            "headers": {},
            "correlation_id": uuid4(),
            "created": now_utc_ms_prec(),
            "published": True,
            "event_id": uuid4(),
        },
    ]

    events_collection.insert_many(unaffected_events)
    await run_db_migrations(config=config, target_version=3)
    results = sorted(events_collection.find({}).to_list(), key=lambda d: d["_id"])
    assert len(results) == 2

    for actual, expected in zip(results, unaffected_events, strict=True):
        assert actual["_id"] == expected["_id"]
        assert actual["payload"] == expected["payload"]
        assert actual["published"] == expected["published"]
        assert actual["type_"] == expected["type_"]
