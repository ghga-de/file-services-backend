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
"""Checking if POST on /secrets works correctly"""

import base64
import os

import pytest
from ghga_service_commons.utils.crypt import encrypt

from tests_ekss.fixtures.config import get_config
from tests_ekss.fixtures.keypair import KeypairFixture
from tests_ekss.fixtures.utils import get_test_client
from tests_ekss.fixtures.vault import VaultFixture

pytestmark = pytest.mark.asyncio()


async def test_post_secrets(*, keypair: KeypairFixture, vault_fixture: VaultFixture):
    """Test request response for /secrets endpoint with valid data"""
    file_secret = os.urandom(32)
    encoded_secret = base64.urlsafe_b64encode(file_secret).decode("utf-8")
    encrypted_secret = encrypt(encoded_secret, key=keypair.ekss_pk, encoding="utf-8")
    config = get_config([vault_fixture.config, keypair.config])
    client = get_test_client(config)
    response = client.post(url="/secrets", content=encrypted_secret)
    assert response.status_code == 200
    body = response.json()
    assert body["secret_id"]
