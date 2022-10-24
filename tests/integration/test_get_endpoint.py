# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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
from fastapi.testclient import TestClient

from ekss.api.download.router import dao_injector
from ekss.api.main import app
from ekss.config import CONFIG
from ekss.core.dao.mongo_db import FileSecretDao

from ..fixtures.dao_keypair import dao_fixture  # noqa: F401
from ..fixtures.dao_keypair import generate_keypair_fixture  # noqa: F401
from ..fixtures.envelope_fixture import envelope_fixture  # noqa: F401
from ..fixtures.envelope_fixture import EnvelopeFixture

client = TestClient(app=app)


@pytest.mark.asyncio
async def test_get_envelope(
    *,
    envelope_fixture: EnvelopeFixture,  # noqa: F811
):
    """Test request response for /secrets/../envelopes/.. endpoint with valid data"""

    async def dao_override() -> FileSecretDao:
        """Ad hoc DAO dependency overridde"""
        return envelope_fixture.dao

    app.dependency_overrides[dao_injector] = dao_override
    secret_id = envelope_fixture.secret_id
    client_pk = base64.urlsafe_b64encode(envelope_fixture.client_pk).decode("utf-8")
    response = client.get(url=f"/secrets/{secret_id}/envelopes/{client_pk}")
    assert response.status_code == 200
    body = response.json()
    content = base64.b64decode(body["content"])
    assert content
    keys = [(0, envelope_fixture.client_sk, None)]
    session_keys, _ = crypt4gh.header.deconstruct(
        infile=io.BytesIO(content),
        keys=keys,
        sender_pubkey=base64.b64decode(CONFIG.server_public_key),
    )
    assert session_keys[0] == envelope_fixture.secret


@pytest.mark.asyncio
async def test_wrong_id(
    *,
    envelope_fixture: EnvelopeFixture,  # noqa: F811
):
    """Test request response for /secrets/../envelopes/.. endpoint with invalid secret_id"""

    async def dao_override() -> FileSecretDao:
        """Ad hoc DAO dependency overridde"""
        return envelope_fixture.dao

    app.dependency_overrides[dao_injector] = dao_override
    secret_id = "wrong_id"
    client_pk = base64.urlsafe_b64encode(envelope_fixture.client_pk).decode("utf-8")
    response = client.get(url=f"/secrets/{secret_id}/envelopes/{client_pk}")
    assert response.status_code == 404
    body = response.json()
    assert body["exception_id"] == "secretNotFoundError"
