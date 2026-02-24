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

"""Logic for testing the V3 migration"""

import json
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

import pytest
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.utils import now_utc_ms_prec
from pytest_httpx import HTTPXMock, httpx_mock  # noqa: F401

from dcs.core.models import FileDownloadServed, FileRegisteredForDownload
from dcs.migrations import run_db_migrations
from tests_dcs.fixtures.config import get_config

pytestmark = pytest.mark.asyncio


def make_pre_v3_drs_object(accession: str) -> dict[str, Any]:
    """Create a DRS object document in pre-v3 format (as it exists after V2 migration)."""
    return {
        "_id": accession,
        "decryption_secret_id": "some-secret",
        "s3_endpoint_alias": "test",
        "decrypted_sha256": "abc123",
        "decrypted_size": 100,
        "encrypted_size": 128,
        "creation_date": now_utc_ms_prec(),
        "object_id": uuid4(),
        "last_accessed": now_utc_ms_prec(),
    }


def make_pre_v3_persisted_event(
    accession: str,
    topic: str,
    type_: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Create a persisted event document in pre-v3 format."""
    return {
        "_id": f"{topic}:{accession}",
        "topic": topic,
        "key": accession,
        "type_": type_,
        "headers": {},
        "payload": payload,
        "correlation_id": uuid4(),
        "created": now_utc_ms_prec(),
        "published": True,
        "event_id": uuid4(),
    }


def derive_file_id_from_accession(accession: str) -> UUID:
    """Use the first portion of the SHA256 hash of an accession to derive a UUID4"""
    hash = sha256(accession.encode()).hexdigest()
    uuid_str = f"{hash[0:8]}-{hash[8:12]}-4{hash[13:16]}-a{hash[17:20]}-{hash[20:32]}"
    new_uuid = UUID(uuid_str)

    # Store the accession's new file ID in the global conversion map
    return new_uuid


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False, can_send_already_matched_responses=True
)
async def test_migration_v3_drs_objects(
    mongodb: MongoDbFixture,
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """Test that DRS object fields are renamed and IDs are derived from accessions."""
    config = get_config(sources=[mongodb.config])
    srs_base = config.srs_base_url

    # Get the DB to the desired starting state
    await run_db_migrations(config=config, target_version=2)

    httpx_mock.add_response(method="GET", url=f"{srs_base}/health", status_code=200)
    httpx_mock.add_response(
        method="POST", url=f"{srs_base}/accession-maps", status_code=204
    )

    accessions = ["GHGAF00001A1", "GHGAF00002A1", "GHGAF00003A1"]
    old_docs = [make_pre_v3_drs_object(acc) for acc in accessions]

    db = mongodb.client[config.db_name]
    drs_collection = db["drs_objects"]
    events_collection = db["dcsPersistedEvents"]
    drs_collection.delete_many({})
    events_collection.delete_many({})
    drs_collection.insert_many(old_docs)

    await run_db_migrations(config=config, target_version=3)

    migrated_docs = drs_collection.find().to_list()
    assert len(migrated_docs) == len(old_docs)

    for old_doc in old_docs:
        accession = old_doc["_id"]
        expected_uuid = derive_file_id_from_accession(accession)
        migrated = drs_collection.find_one({"_id": expected_uuid})

        assert migrated is not None, f"No migrated doc found for accession {accession}"
        assert isinstance(migrated["_id"], UUID)
        assert migrated["_id"] == expected_uuid

        # Old field names must be gone, new ones present
        assert "secret_id" in migrated
        assert "storage_alias" in migrated
        assert "decryption_secret_id" not in migrated
        assert "s3_endpoint_alias" not in migrated

        # Values carried over
        assert migrated["secret_id"] == old_doc["decryption_secret_id"]
        assert migrated["storage_alias"] == old_doc["s3_endpoint_alias"]
        assert migrated["decrypted_sha256"] == old_doc["decrypted_sha256"]
        assert migrated["object_id"] == old_doc["object_id"]


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False, can_send_already_matched_responses=True
)
async def test_migration_v3_persisted_events(
    mongodb: MongoDbFixture,
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """Test that persisted event IDs, keys, and payload fields are correctly transformed."""
    config = get_config(sources=[mongodb.config])
    srs_base = config.srs_base_url
    # Get the DB to the desired starting state
    await run_db_migrations(config=config, target_version=2)

    httpx_mock.add_response(method="GET", url=f"{srs_base}/health", status_code=200)
    httpx_mock.add_response(
        method="POST", url=f"{srs_base}/accession-maps", status_code=204
    )

    # Use distinct accessions so compaction keys (_id) are unique even though all
    # events share the same topic (file-downloads in the test config).
    accession_served = "GHGAF88881A1"
    uuid_served = derive_file_id_from_accession(accession_served)
    accession_registered = "GHGAF88882A1"
    uuid_registered = derive_file_id_from_accession(accession_registered)
    accession_deleted = "GHGAF88883A1"
    uuid_deleted = derive_file_id_from_accession(accession_deleted)
    upload_date = now_utc_ms_prec()

    old_object_id = uuid4()

    old_drs_docs = [
        make_pre_v3_drs_object(accession_served),
        make_pre_v3_drs_object(accession_registered),
        make_pre_v3_drs_object(accession_deleted),
    ]

    old_events = [
        make_pre_v3_persisted_event(
            accession=accession_served,
            topic=config.download_served_topic,
            type_="download_served",
            payload={
                "file_id": accession_served,
                "s3_endpoint_alias": "HD01",
                "target_object_id": old_object_id,
                "target_bucket_id": "some-bucket",
                "decrypted_sha256": "a1b2c3",
                "context": "",
            },
        ),
        make_pre_v3_persisted_event(
            accession=accession_registered,
            topic=config.file_registered_for_download_topic,
            type_="file_registered",
            payload={
                "file_id": accession_registered,
                "upload_date": upload_date,
                "decrypted_sha256": "a1b2c3",
                "drs_uri": "drs://localhost:8080/ga4gh/drs/v1",
            },
        ),
        make_pre_v3_persisted_event(
            accession=accession_deleted,
            topic=config.file_deleted_topic,
            type_="file_deleted",
            payload={
                "file_id": accession_deleted,
            },
        ),
    ]

    db = mongodb.client[config.db_name]
    drs_collection = db["drs_objects"]
    events_collection = db["dcsPersistedEvents"]
    drs_collection.delete_many({})
    events_collection.delete_many({})
    drs_collection.insert_many(old_drs_docs)
    events_collection.insert_many(old_events)

    await run_db_migrations(config=config, target_version=3)

    migrated_events = events_collection.find().to_list()
    assert len(migrated_events) == len(old_events)

    # Check common transformations on all events
    for old_event in old_events:
        accession = old_event["key"]
        expected_uuid_str = str(derive_file_id_from_accession(accession))

        migrated = events_collection.find_one({"type_": old_event["type_"]})
        assert migrated is not None

        assert accession not in migrated["_id"]
        assert expected_uuid_str in migrated["_id"]
        assert migrated["key"] == expected_uuid_str
        assert migrated["published"] is False

    # download_served: s3_endpoint_alias → storage_alias, object_id → file_id
    served = events_collection.find_one({"type_": "download_served"})
    expected_served = FileDownloadServed(
        file_id=uuid_served,
        storage_alias="HD01",
        target_bucket_id="some-bucket",
        target_object_id=old_object_id,
        decrypted_sha256="a1b2c3",
        context="",
    )
    assert served is not None
    assert served["payload"] == expected_served.model_dump()

    # file_registered: drs_uri removed
    registered = events_collection.find_one({"type_": "file_registered"})
    expected_registered = FileRegisteredForDownload(
        file_id=uuid_registered, decrypted_sha256="a1b2c3", archive_date=upload_date
    )
    assert registered is not None
    assert registered["payload"] == expected_registered.model_dump()

    # file_deleted: empty payload unchanged
    deleted = events_collection.find_one({"type_": "file_deleted"})
    assert deleted is not None
    assert deleted["payload"] == {"file_id": uuid_deleted}


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False, can_send_already_matched_responses=True
)
@pytest.mark.parametrize("count", [100, 2001])
async def test_migration_v3_accession_map_posts(
    count: int,
    mongodb: MongoDbFixture,
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """Test that accession-to-UUID maps are POSTed to the SRS in the correct format.

    Verifies that:
    - Exactly one POST is made when item count is within a single batch (< 500).
    - Multiple batches are made when item count is 500 or greater.
    - Each POST body is a dict[str, str] mapping accessions to UUID4 strings.
    - All accessions are included and values round-trip to valid UUID4s.
    """
    config = get_config(sources=[mongodb.config])
    srs_base = config.srs_base_url
    # Get the DB to the desired starting state
    await run_db_migrations(config=config, target_version=2)

    httpx_mock.add_response(method="GET", url=f"{srs_base}/health", status_code=200)
    httpx_mock.add_response(
        method="POST", url=f"{srs_base}/accession-maps", status_code=204
    )

    accessions = [f"GHGAF{i:05d}A1" for i in range(count)]
    old_docs = [make_pre_v3_drs_object(acc) for acc in accessions]

    db = mongodb.client[config.db_name]
    drs_collection = db["drs_objects"]
    events_collection = db["dcsPersistedEvents"]
    drs_collection.delete_many({})
    events_collection.delete_many({})
    drs_collection.insert_many(old_docs)

    await run_db_migrations(config=config, target_version=3)

    post_requests = httpx_mock.get_requests(method="POST")
    expected_batch_count = -(-count // 500)  # ceiling division
    assert len(post_requests) == expected_batch_count

    # Merge all batches and verify the combined map is complete and correct
    combined_map: dict[str, str] = {}
    for request in post_requests:
        batch: dict[str, str] = json.loads(request.content)
        assert isinstance(batch, dict)
        assert len(batch) <= 500
        combined_map.update(batch)

    assert len(combined_map) == count
    for accession in accessions:
        expected_uuid = derive_file_id_from_accession(accession)
        assert accession in combined_map
        assert combined_map[accession] == str(expected_uuid)
        assert UUID(combined_map[accession]).version == 4


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False, can_send_already_matched_responses=True
)
@pytest.mark.parametrize("bad_status_code", [200, 400, 500])
async def test_migration_v3_srs_error(
    bad_status_code: int,
    mongodb: MongoDbFixture,
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """Test that a non-204 response from the SRS POST endpoint raises a RuntimeError."""
    config = get_config(sources=[mongodb.config])
    srs_base = config.srs_base_url
    await run_db_migrations(config=config, target_version=2)

    db = mongodb.client[config.db_name]
    drs_collection = db["drs_objects"]
    events_collection = db["dcsPersistedEvents"]
    drs_collection.delete_many({})
    events_collection.delete_many({})
    drs_collection.insert_one(make_pre_v3_drs_object("GHGA001"))

    msg = "Unable to apply DB version 3 (V3Migration)"
    # Should fail when status code from health endpoint is not 200
    httpx_mock.add_response(method="GET", url=f"{srs_base}/health", status_code=503)
    with pytest.raises(RuntimeError, check=lambda e: str(e) == msg):
        await run_db_migrations(config=config, target_version=3)

    # Make it so the next call to the health endpoint is...healthy
    httpx_mock.add_response(method="GET", url=f"{srs_base}/health", status_code=200)

    # Should fail when accession map endpoint doesn't return a 204
    httpx_mock.add_response(
        method="POST", url=f"{srs_base}/accession-maps", status_code=bad_status_code
    )
    with pytest.raises(RuntimeError, check=lambda e: str(e) == msg):
        await run_db_migrations(config=config, target_version=3)
