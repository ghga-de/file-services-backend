# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Mock EKSS endpoints"""

import httpx2
from fastapi import status
from ghga_service_commons.api.mock_router import MockRouter

DEPOSITED_SECRET_ID = "some-secret-id"

router = MockRouter()


@router.post("/ekss/secrets")
def ekss_deposit_secret_mock():
    """Mock API call to the EKSS to deposit a file secret"""
    return httpx2.Response(
        status_code=status.HTTP_201_CREATED, json={"secret_id": DEPOSITED_SECRET_ID}
    )


@router.delete("/ekss/secrets/{secret_id}")
def ekss_delete_secret_mock(secret_id: str):
    """Mock API call to the EKSS to delete a file secret"""
    return httpx2.Response(status_code=status.HTTP_204_NO_CONTENT)
