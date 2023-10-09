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

import pytest
from ghga_service_commons.utils.crypt import encrypt, generate_key_pair

from fis.core.models import FileUploadMetadataEncrypted
from tests.fixtures.joint import (  # noqa: F401
    JointFixture,
    KafkaFixture,
    joint_fixture,
    kafka_fixture,
)


@pytest.mark.asyncio
async def test_decryption_happy(joint_fixture: JointFixture):  # noqa: F811
    """Test decryption with valid keypair and correct file upload metadata format."""
    upload_metadata_processor = (
        await joint_fixture.container.upload_metadata_processor()
    )

    processed_payload = await upload_metadata_processor.decrypt_payload(
        encrypted=joint_fixture.encrypted_payload
    )
    assert processed_payload == joint_fixture.payload


@pytest.mark.asyncio
async def test_decryption_sad(joint_fixture: JointFixture):  # noqa: F811
    """Test decryption throws correct errors for payload and key issues"""
    upload_metadata_processor = (
        await joint_fixture.container.upload_metadata_processor()
    )

    # test faulty payload
    encrypted_payload = FileUploadMetadataEncrypted(
        payload=encrypt(
            data=joint_fixture.payload.json(exclude={"file_secret"}),
            key=joint_fixture.keypair.public,
        )
    )

    with pytest.raises(upload_metadata_processor.WrongDecryptedFormatError):
        await upload_metadata_processor.decrypt_payload(encrypted=encrypted_payload)

    # test wrong key
    keypair2 = generate_key_pair()

    encrypted_payload = FileUploadMetadataEncrypted(
        payload=encrypt(data=joint_fixture.payload.json(), key=keypair2.public)
    )

    with pytest.raises(upload_metadata_processor.DecryptionError):
        await upload_metadata_processor.decrypt_payload(encrypted=encrypted_payload)
