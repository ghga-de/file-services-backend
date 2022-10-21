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
"""Checking if POST on /secrets works correctly"""
import base64
import codecs
import io

import crypt4gh.header
import pytest
from fastapi.testclient import TestClient

from ekss.api.main import app
from ekss.api.upload.router import dao_injector
from ekss.config import CONFIG
from ekss.core.dao.mongo_db import FileSecretDao

from ..fixtures.dao_keypair import dao_fixture  # noqa: F401
from ..fixtures.dao_keypair import generate_keypair_fixture  # noqa: F401
from ..fixtures.file_fixture import first_part_fixture  # noqa: F401
from ..fixtures.file_fixture import FirstPartFixture

client = TestClient(app=app)


@pytest.mark.asyncio
async def test_post_secrets(
    *,
    first_part_fixture: FirstPartFixture,  # noqa: F811
):
    """Test request response for /secrets endpoint with valid data"""

    async def dao_override() -> FileSecretDao:
        """Ad hoc DAO dependency overridde"""
        return first_part_fixture.dao

    app.dependency_overrides[dao_injector] = dao_override

    payload = first_part_fixture.content

    request_body = {
        "public_key": base64.b64encode(first_part_fixture.client_pubkey).hex(),
        "file_part": base64.b64encode(payload).hex(),
    }
    response = client.post(url="/secrets", json=request_body)
    assert response.status_code == 200
    body = response.json()
    secret = base64.b64decode(codecs.decode(body["secret"], "hex"))

    server_private_key = base64.b64decode(CONFIG.server_private_key.get_secret_value())
    # (method - only 0 supported for now, private_key, public_key)
    keys = [(0, server_private_key, None)]
    session_keys, _ = crypt4gh.header.deconstruct(
        io.BytesIO(payload), keys, sender_pubkey=first_part_fixture.client_pubkey
    )

    assert secret == session_keys[0]
    assert body["secret_id"]
    assert body["offset"] > 0


@pytest.mark.asyncio
async def test_corrupted_header(
    *,
    first_part_fixture: FirstPartFixture,  # noqa: F811
):
    """Test request response for /secrets endpoint with first char replaced in envelope"""

    async def dao_override() -> FileSecretDao:
        """Ad hoc DAO dependency overridde"""
        return first_part_fixture.dao

    app.dependency_overrides[dao_injector] = dao_override

    payload = b"k" + first_part_fixture.content[2:]
    content = base64.b64encode(payload).hex()

    request_body = {
        "public_key": base64.b64encode(first_part_fixture.client_pubkey).hex(),
        "file_part": content,
    }

    response = client.post(url="/secrets", json=request_body)
    assert response.status_code == 400
    body = response.json()
    assert body["exception_id"] == "malformedOrMissingEnvelopeError"


@pytest.mark.asyncio
async def test_missing_envelope(
    *,
    first_part_fixture: FirstPartFixture,  # noqa: F811
):
    """Test request response for /secrets endpoint without envelope"""

    async def dao_override() -> FileSecretDao:
        """Ad hoc DAO dependency overridde"""
        return first_part_fixture.dao

    app.dependency_overrides[dao_injector] = dao_override

    payload = first_part_fixture.content
    content = base64.b64encode(payload).hex()
    content = content[124:]

    request_body = {
        "public_key": base64.b64encode(first_part_fixture.client_pubkey).hex(),
        "file_part": content,
    }

    response = client.post(url="/secrets", json=request_body)
    assert response.status_code == 400
    body = response.json()
    assert body["exception_id"] == "malformedOrMissingEnvelopeError"
