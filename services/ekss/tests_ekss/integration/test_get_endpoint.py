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
"""Checking if GET on /secrets/{secret_id}/envelopes/{client_pk} works correctly"""

import base64
import io

import crypt4gh.header
import pytest

from tests_ekss.fixtures.envelope import EnvelopeFixture
from tests_ekss.fixtures.keypair import KeypairFixture
from tests_ekss.fixtures.utils import get_test_client

pytestmark = pytest.mark.asyncio()


async def test_get_envelope(keypair: KeypairFixture, envelope_fixture: EnvelopeFixture):
    """Test request response for /secrets/../envelopes/.. endpoint with valid data"""
    secret_id = envelope_fixture.secret_id
    client_pk = base64.urlsafe_b64encode(envelope_fixture.user_public_key).decode(
        "utf-8"
    )
    client = get_test_client(envelope_fixture.config)
    response = client.get(url=f"/secrets/{secret_id}/envelopes/{client_pk}")
    assert response.status_code == 200
    body = response.json()
    content = base64.b64decode(body["content"])
    assert content
    keys = [(0, keypair.user_sk, None)]
    session_keys, _ = crypt4gh.header.deconstruct(
        infile=io.BytesIO(content),
        keys=keys,
        sender_pubkey=keypair.ekss_pk,
    )
    assert session_keys[0] == envelope_fixture.secret


async def test_wrong_id(envelope_fixture: EnvelopeFixture):
    """Test request response for /secrets/../envelopes/.. endpoint with invalid secret_id"""
    secret_id = "wrong_id"
    client_pk = base64.urlsafe_b64encode(envelope_fixture.user_public_key).decode(
        "utf-8"
    )
    client = get_test_client(envelope_fixture.config)
    response = client.get(url=f"/secrets/{secret_id}/envelopes/{client_pk}")
    assert response.status_code == 404
    body = response.json()
    assert body["exception_id"] == "secretNotFoundError"
