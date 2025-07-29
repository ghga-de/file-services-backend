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
#
"""Test utilities to create temporary files."""

import hashlib
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import crypt4gh.header
import crypt4gh.lib
from ghga_service_commons.utils.temp_files import big_temp_file
from hexkit.providers.s3.testutils import FileObject
from hexkit.utils import now_utc_ms_prec

from tests_irs.fixtures.joint import FILE_SIZE, S3Fixture


@dataclass
class EncryptedData:
    """Object metadata for testing purposes"""

    checksum: str
    file_id: str
    file_object: FileObject
    file_secret: bytes
    file_size: int
    upload_date: str
    offset: int


async def create_test_file(
    bucket_id: str, private_key: bytes, public_key: bytes, s3: S3Fixture
):
    """Generate encrypted random test data using a specified keypair"""
    sys.set_int_max_str_digits(FILE_SIZE)
    with big_temp_file(FILE_SIZE) as data:
        # rewind data pointer
        data.seek(0)
        with tempfile.NamedTemporaryFile() as encrypted_file:
            enc_keys = [(0, private_key, public_key)]

            crypt4gh.lib.encrypt(keys=enc_keys, infile=data, outfile=encrypted_file)

            # get unencrypted checksum
            data.seek(0)
            checksum = hashlib.sha256(data.read()).hexdigest()

            encrypted_file.seek(0)
            dec_keys = [(0, private_key, None)]
            session_keys, _ = crypt4gh.header.deconstruct(
                infile=encrypted_file, keys=dec_keys, sender_pubkey=public_key
            )
            file_secret = session_keys[0]

            offset = encrypted_file.tell()
            # Rewind file
            encrypted_file.seek(0)
            object_id = str(uuid4())
            file_id = f"F{object_id}"
            file_object = FileObject(
                file_path=Path(encrypted_file.name),
                bucket_id=bucket_id,
                object_id=object_id,
            )
            await s3.populate_file_objects([file_object])

            return EncryptedData(
                checksum=checksum,
                file_id=file_id,
                file_object=file_object,
                file_secret=file_secret,
                file_size=len(file_object.content),
                offset=offset,
                upload_date=now_utc_ms_prec().isoformat(),
            )
