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

"""Implements functionality for envelope encryption"""

from pathlib import Path

import crypt4gh.header
from crypt4gh.keys import get_private_key

from ekss.adapters.outbound.vault import VaultAdapter


async def get_envelope(
    *,
    secret_id: str,
    client_pubkey: bytes,
    vault: VaultAdapter,
    server_private_key_path: Path,
    passphrase: str | None,
) -> bytes:
    """Calls the database and then calls a function to assemble an envelope"""
    file_secret = vault.get_secret(key=secret_id)
    header_envelope = await create_envelope(
        file_secret=file_secret,
        client_pubkey=client_pubkey,
        server_private_key_path=server_private_key_path,
        passphrase=passphrase,
    )

    return header_envelope


async def create_envelope(
    *,
    file_secret: bytes,
    client_pubkey: bytes,
    server_private_key_path: Path,
    passphrase: str | None,
) -> bytes:
    """
    Gather file encryption/decryption secret and assemble a crypt4gh envelope using the
    servers private and the clients public key
    """
    server_private_key = get_private_key(
        server_private_key_path, callback=lambda: passphrase
    )
    keys = [(0, server_private_key, client_pubkey)]
    header_content = crypt4gh.header.make_packet_data_enc(0, file_secret)
    header_packets = crypt4gh.header.encrypt(header_content, keys)
    header_bytes = crypt4gh.header.serialize(header_packets)

    return header_bytes
