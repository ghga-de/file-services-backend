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
"""Set up session-scope fixtures for tests."""

import pytest
import pytest_asyncio
from hexkit.providers.akafka.testutils import (  # noqa: F401
    kafka_container_fixture,
    kafka_fixture,
)
from hexkit.providers.mongodb.testutils import (  # noqa: F401
    mongodb_container_fixture,
    mongodb_fixture,
)
from hexkit.providers.s3.testutils import (  # noqa: F401
    S3Fixture,
    s3_container_fixture,
    s3_fixture,
)

from tests_irs.fixtures.joint import (  # noqa: F401
    INBOX_BUCKET_ID,
    STAGING_BUCKET_ID,
    JointFixture,
    joint_fixture,
)
from tests_irs.fixtures.keypair_fixtures import keypair_fixture  # noqa: F401


async def _populate_s3_buckets(s3: S3Fixture):
    await s3.populate_buckets([INBOX_BUCKET_ID, STAGING_BUCKET_ID])


def get_populate_s3_buckets_fixture(name: str = "populate_s3_buckets"):
    """Populate the S3 instance buckets"""
    return pytest_asyncio.fixture(
        _populate_s3_buckets, scope="function", name=name, autouse=True
    )


populate_s3_buckets = get_populate_s3_buckets_fixture()


@pytest.fixture(scope="function", autouse=True)
def regenerate_keypair(joint_fixture: JointFixture):  # noqa: F811
    """Regenerate the keypair for each test."""
    joint_fixture.keypair.regenerate()
