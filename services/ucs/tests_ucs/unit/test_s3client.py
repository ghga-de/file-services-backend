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

"""Unit tests for the S3Client"""

from uuid import uuid4

import pytest
import pytest_asyncio
from ghga_service_commons.utils.multinode_storage import ObjectStorages
from hexkit.providers.testing.s3 import InMemObjectStorage
from hexkit.utils import now_utc_ms_prec
from pydantic import UUID4

from tests_ucs.fixtures import ConfigFixture
from tests_ucs.fixtures.in_mem_obj_storage import InMemS3ObjectStorages
from ucs.adapters.outbound.s3 import S3Client
from ucs.core.models import S3UploadDetails
from ucs.ports.outbound.storage import S3ClientPort

TEST_STORAGE_ALIAS = "test"  # Should match the test config
TEST_BUCKET = "test-inbox"


def make_s3_upload_details(
    *,
    file_id: UUID4 | None = None,
    s3_upload_id: str = "",
    object_id: UUID4 | None = None,
) -> S3UploadDetails:
    """Make an instance of S3UploadDetails"""
    file_id = file_id or uuid4()
    object_id = object_id or uuid4()
    return S3UploadDetails(
        file_id=file_id,
        storage_alias=TEST_STORAGE_ALIAS,
        bucket_id=TEST_BUCKET,
        object_id=object_id,
        s3_upload_id=s3_upload_id,
        initiated=now_utc_ms_prec(),
    )


@pytest.fixture()
def patch_s3_calls(monkeypatch):
    """Mocks the object storage provider with an InMemObjectStorage object"""
    pass
    monkeypatch.setattr(
        f"{InMemS3ObjectStorages.__module__}.S3ObjectStorage", InMemObjectStorage
    )


@pytest.fixture(name="object_storages")
def configured_object_storages(config: ConfigFixture, patch_s3_calls) -> ObjectStorages:
    """Return a configured InMemObjectStorages instance."""
    return InMemS3ObjectStorages(config=config.config)


@pytest.fixture(name="s3_client")
def configured_s3_client(config: ConfigFixture, object_storages) -> S3ClientPort:
    """Return a configured S3Client instance plugged into an in-mem object storage."""
    return S3Client(config=config.config, object_storages=object_storages)


@pytest_asyncio.fixture(autouse=True)
async def create_default_bucket(object_storages: ObjectStorages):
    """Create the `test-inbox` bucket automatically for tests."""
    await object_storages.for_alias(TEST_STORAGE_ALIAS)[1].create_bucket(TEST_BUCKET)


@pytest.mark.asyncio
async def test_get_part_upload_url_when_s3_upload_not_found(
    s3_client: S3ClientPort, object_storages: ObjectStorages
):
    """Test for error handling when getting a part URL but S3 raises an error saying
    that it can't find the corresponding multipart upload on its end.
    """
    # Try to get a part upload URL - should raise S3UploadNotFoundError
    s3_upload_details = make_s3_upload_details(s3_upload_id="not-real")

    with pytest.raises(S3ClientPort.S3UploadNotFoundError):
        await s3_client.get_part_upload_url(
            s3_upload_details=s3_upload_details, part_no=123
        )
