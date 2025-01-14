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

"""Unit tests for upload functionality"""

import pytest

from ekss.core.envelope_decryption import extract_envelope_content
from tests_ekss.fixtures.file import (
    FirstPartFixture,
    first_part_fixture,  # noqa: F401
)
from tests_ekss.fixtures.keypair import tmp_keypair  # noqa: F401
from tests_ekss.fixtures.vault import vault_fixture  # noqa: F401


@pytest.mark.asyncio
async def test_extract(
    *,
    first_part_fixture: FirstPartFixture,  # noqa: F811
):
    """Test envelope extraction/file secret insertion"""
    client_pubkey = first_part_fixture.client_pubkey

    config = first_part_fixture.config
    submitter_secret, offset = await extract_envelope_content(
        file_part=first_part_fixture.content,
        client_pubkey=client_pubkey,
        server_private_key_path=config.server_private_key_path,
        passphrase=config.private_key_passphrase,
    )

    secret_id = first_part_fixture.vault.adapter.store_secret(secret=submitter_secret)
    result = (submitter_secret, secret_id, offset)

    assert all(result)
    assert offset > 0
