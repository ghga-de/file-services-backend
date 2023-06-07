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

"""Test ingest functions"""

import base64
import os

import pytest
from ghga_service_commons.utils.crypt import encode_key, encrypt, generate_key_pair

from fis.config import ServiceConfig
from fis.core.models import FileUploadMetadata, FileUploadMetadataEncrypted
from fis.main import get_configured_container
from tests.fixtures.config import get_config


@pytest.mark.asyncio
async def test_decryption_happy():
    """Test decryption with valid keypair and correct file upload metadata format."""

    # setup, move into fixture later
    keypair = generate_key_pair()
    private_key = encode_key(key=keypair.private)

    patched_service_config = ServiceConfig(token_hashes=[], private_key=private_key)
    config = get_config(sources=[patched_service_config])
    container = get_configured_container(config=config)
    container.wire(modules=["fis.adapters.inbound.fastapi_.routes"])

    # actual test code
    upload_metadata_processor = container.upload_metadata_processor()

    decrypted_payload = FileUploadMetadata(
        file_id="abc",
        object_id="happy_little_object",
        part_size=16 * 1024**2,
        unencrypted_size=50 * 1024**2,
        encrypted_size=50 * 1024**2 + 128,
        file_secret=base64.b64encode(os.urandom(32)).decode(),
        unencrypted_checksum="def",
        encrypted_md5_checksums=["a", "b", "c"],
        encrypted_sha256_checksums=["a", "b", "c"],
    )

    encrypted_payload = FileUploadMetadataEncrypted(
        payload=encrypt(data=decrypted_payload.json(), key=keypair.public)
    )

    processed_payload = await upload_metadata_processor.decrypt_payload(
        encrypted=encrypted_payload
    )
    assert processed_payload == decrypted_payload


@pytest.mark.asyncio
async def test_decryption_sad():
    """Test decryption throws correct errors for payload and key issues"""

    # setup, move into fixture later
    keypair = generate_key_pair()
    private_key = encode_key(key=keypair.private)

    patched_service_config = ServiceConfig(token_hashes=[], private_key=private_key)
    config = get_config(sources=[patched_service_config])
    container = get_configured_container(config=config)
    container.wire(modules=["fis.adapters.inbound.fastapi_.routes"])

    # actual test code
    decrypted_payload = FileUploadMetadata(
        file_id="abc",
        object_id="happy_little_object",
        part_size=16 * 1024**2,
        unencrypted_size=50 * 1024**2,
        encrypted_size=50 * 1024**2 + 128,
        file_secret=base64.b64encode(os.urandom(32)).decode(),
        unencrypted_checksum="def",
        encrypted_md5_checksums=["a", "b", "c"],
        encrypted_sha256_checksums=["a", "b", "c"],
    )

    upload_metadata_processor = container.upload_metadata_processor()

    # test faulty payload
    encrypted_payload = FileUploadMetadataEncrypted(
        payload=encrypt(
            data=decrypted_payload.json(exclude={"file_secret"}), key=keypair.public
        )
    )

    with pytest.raises(upload_metadata_processor.WrongDecryptedFormatError):
        await upload_metadata_processor.decrypt_payload(encrypted=encrypted_payload)

    # test wrong key
    keypair2 = generate_key_pair()

    encrypted_payload = FileUploadMetadataEncrypted(
        payload=encrypt(data=decrypted_payload.json(), key=keypair2.public)
    )

    with pytest.raises(upload_metadata_processor.DecryptionError):
        await upload_metadata_processor.decrypt_payload(encrypted=encrypted_payload)
