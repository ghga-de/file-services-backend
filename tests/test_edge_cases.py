# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

import pytest
from fastapi import status
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401

from dcs.config import WorkOrderTokenConfig
from dcs.container import auth_provider
from tests.fixtures.joint import *  # noqa: F403


@pytest.mark.asyncio
async def test_get_health(joint_fixture: JointFixture):  # noqa: F811, F405
    """Test the GET /health endpoint"""

    response = await joint_fixture.rest_client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "OK"}


@pytest.mark.asyncio
async def test_access_non_existing(joint_fixture: JointFixture):  # noqa F811
    """Checks that requesting access to a non-existing DRS object fails with the
    expected exception."""

    file_id = "my-non-existing-id"

    # request access to non existing DRS object:
    work_order_token, pubkey = get_work_order_token(file_id=file_id)  # noqa: F405

    # test with missing authorization header
    response = await joint_fixture.rest_client.get(f"/objects/{file_id}")
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # test with authorization header but wrong pubkey
    response = await joint_fixture.rest_client.get(
        f"/objects/{file_id}",
        timeout=5,
        headers={"Authorization": f"Bearer {work_order_token}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # update pubkey config option on live object
    auth_provider_override = auth_provider(config=WorkOrderTokenConfig(auth_key=pubkey))

    with joint_fixture.container.auth_provider.override(auth_provider_override):
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
