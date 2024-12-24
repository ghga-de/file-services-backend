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

"""Tests edge cases not covered by the typical journey test."""

import re
from dataclasses import dataclass
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from fastapi import status
from ghga_service_commons.api.testing import AsyncTestClient
from ghga_service_commons.utils.utc_dates import now_as_utc
from pytest_httpx import HTTPXMock, httpx_mock  # noqa: F401

from dcs.adapters.inbound.fastapi_.routes import router
from dcs.config import Config
from dcs.core import models
from dcs.inject import prepare_rest_app
from dcs.ports.outbound.dao import DrsObjectDaoPort
from tests_dcs.fixtures.joint import EXAMPLE_FILE, JointFixture, PopulatedFixture
from tests_dcs.fixtures.utils import (
    generate_token_signing_keys,
    generate_work_order_token,
)

pytestmark = pytest.mark.asyncio()


@dataclass
class StorageUnavailableFixture:
    """Fixture to provide DRS DB entry with misconfigured storage alias"""

    mongodb_dao: DrsObjectDaoPort
    joint: JointFixture
    file_id: str


@pytest_asyncio.fixture
async def storage_unavailable_fixture(joint_fixture: JointFixture):
    """Set up file with unavailable storage alias"""
    alias = joint_fixture.endpoint_aliases.fake

    test_file = EXAMPLE_FILE.model_copy(deep=True)
    test_file.file_id = alias
    test_file.object_id = alias
    test_file.s3_endpoint_alias = alias

    # populate DB entry
    mongodb_dao = await joint_fixture.mongodb.dao_factory.get_dao(
        name="drs_objects",
        dto_model=models.AccessTimeDrsObject,
        id_field="file_id",
    )
    await mongodb_dao.insert(test_file)

    yield StorageUnavailableFixture(
        mongodb_dao=mongodb_dao,
        joint=joint_fixture,
        file_id=test_file.file_id,
    )


async def test_get_health(joint_fixture: JointFixture):
    """Test the GET /health endpoint"""
    response = await joint_fixture.rest_client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "OK"}


async def test_access_non_existing(joint_fixture: JointFixture):
    """Checks that requesting access to a non-existing DRS object fails with the
    expected exception.
    """
    file_id = "my-non-existing-id"

    work_order_token = generate_work_order_token(file_id=file_id, jwk=joint_fixture.jwk)
    wrong_jwk = generate_token_signing_keys()
    wrong_work_order_token = generate_work_order_token(file_id=file_id, jwk=wrong_jwk)

    # test with missing authorization header
    # (should not expose whether the file with the given id exists or not)
    response = await joint_fixture.rest_client.get(
        f"/objects/{file_id}",
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # test with authorization header but wrong pubkey
    response = await joint_fixture.rest_client.get(
        f"/objects/{file_id}",
        timeout=5,
        headers={"Authorization": f"Bearer {wrong_work_order_token}"},
    )
    assert response.status_code == status.HTTP_401_UNAUTHORIZED

    # test with correct authorization header but wrong object_id
    response = await joint_fixture.rest_client.get(
        f"/objects/{file_id}",
        timeout=5,
        headers={"Authorization": f"Bearer {work_order_token}"},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND

    response = await joint_fixture.rest_client.get(
        f"/objects/{file_id}/envelopes",
        timeout=5,
        headers={"Authorization": f"Bearer {work_order_token}"},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False, can_send_already_matched_responses=True
)
async def test_deletion_config_error(
    storage_unavailable_fixture: StorageUnavailableFixture,
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """Simulate a deletion request for a file with an unconfigured storage alias."""
    # explicitly handle ekss API calls (and name unintercepted hosts above)
    httpx_mock.add_callback(
        callback=router.handle_request,
        url=re.compile(rf"^{storage_unavailable_fixture.joint.config.ekss_base_url}.*"),
    )

    data_repository = storage_unavailable_fixture.joint.data_repository
    with pytest.raises(data_repository.StorageAliasNotConfiguredError):
        await data_repository.delete_file(file_id=storage_unavailable_fixture.file_id)


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False, can_send_already_matched_responses=True
)
async def test_drs_config_error(
    storage_unavailable_fixture: StorageUnavailableFixture,
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """Test DRS endpoint for a storage alias that is not configured"""
    # generate work order token
    work_order_token = generate_work_order_token(
        file_id=storage_unavailable_fixture.file_id,
        jwk=storage_unavailable_fixture.joint.jwk,
        valid_seconds=120,
    )

    # modify default headers:
    storage_unavailable_fixture.joint.rest_client.headers = httpx.Headers(
        {"Authorization": f"Bearer {work_order_token}"}
    )

    # explicitly handle ekss API calls (and name unintercepted hosts above)
    httpx_mock.add_callback(
        callback=router.handle_request,
        url=re.compile(rf"^{storage_unavailable_fixture.joint.config.ekss_base_url}.*"),
    )

    drs_id = storage_unavailable_fixture.file_id
    response = await storage_unavailable_fixture.joint.rest_client.get(
        f"/objects/{drs_id}", timeout=5
    )
    assert response.status_code == 500


async def test_register_file_twice(populated_fixture: PopulatedFixture, caplog):
    """Assure that files cannot be registered twice"""
    joint_fixture = populated_fixture.joint_fixture
    example_file = populated_fixture.example_file

    file = models.DrsObjectBase(
        file_id=example_file.file_id,
        decryption_secret_id=example_file.decryption_secret_id,
        decrypted_sha256=example_file.decrypted_sha256,
        decrypted_size=example_file.decrypted_size,
        creation_date=now_as_utc().isoformat(),
        s3_endpoint_alias=example_file.s3_endpoint_alias,
    )

    caplog.clear()
    await joint_fixture.data_repository.register_new_file(file=file)
    failure_message = f"Could not register file with id '{
        example_file.file_id}' as an entry already exists for this id."
    assert failure_message in caplog.messages


@pytest.mark.parametrize(
    "url_validity, buffer, expected_url_max_age",
    [
        (60, 10, 50),
        (15, 10, 10),
        (5, 10, 10),
    ],
    ids=["Normal", "UseBufferAsMinimum", "ValidityLessThanBuffer"],
)
async def test_cache_headers(
    joint_fixture: JointFixture,
    url_validity: int,
    buffer: int,
    expected_url_max_age: int,
    monkeypatch,
):
    """Test that the cache headers are set correctly"""
    joint_config = joint_fixture.config.model_dump()
    joint_config.update(
        presigned_url_expires_after=url_validity,
        url_expiration_buffer=buffer,
    )
    config = Config(**joint_config)
    work_order_token = generate_work_order_token(
        file_id="test-file",
        jwk=joint_fixture.jwk,
        valid_seconds=120,
    )
    headers = httpx.Headers({"Authorization": f"Bearer {work_order_token}"})
    monkeypatch.setattr(
        joint_fixture.data_repository,
        "access_drs_object",
        AsyncMock(return_value=EXAMPLE_FILE),
    )
    async with prepare_rest_app(
        config=config, data_repo_override=joint_fixture.data_repository
    ) as app:
        async with AsyncTestClient(app) as client:
            response = await client.get("/objects/test-file", headers=headers)

        assert "Cache-Control" in response.headers
        assert response.headers["Cache-Control"] == f"max-age={expected_url_max_age}"
