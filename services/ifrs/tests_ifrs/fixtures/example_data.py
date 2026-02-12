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

"""Example data used for testing."""

from datetime import timedelta
from uuid import uuid4

from hexkit.utils import now_utc_ms_prec

from ifrs.core.models import AccessionedFileUpload, FileUpload, PendingFileUpload
from tests_ifrs.fixtures.joint import INTERROGATION_BUCKET

EXAMPLE_FILE_UPLOAD_INBOX = FileUpload(
    id=uuid4(),
    storage_alias="HD01",
    bucket_id="inbox",
    state="inbox",
    state_updated=now_utc_ms_prec() - timedelta(hours=1),
    decrypted_size=64 * 1024**2,
    encrypted_size=64 * 1024**2 + 1234567,
    part_size=16 * 1024**2,
)

# This is the event received by the outbox subscriber when the file is ready
EXAMPLE_ARCHIVABLE_FILE = FileUpload(
    id=EXAMPLE_FILE_UPLOAD_INBOX.id,
    storage_alias="HD01",
    state="awaiting_archival",
    state_updated=now_utc_ms_prec() - timedelta(hours=1),
    bucket_id=INTERROGATION_BUCKET,
    secret_id="some-secret-id",
    decrypted_size=64 * 1024**2,
    encrypted_size=64 * 1024**2 + 1234567,
    part_size=16 * 1024**2,
    # The checksums are only examples, they don't correspond to a particular file:
    decrypted_sha256="0677de3685577a06862f226bb1bfa8f889e96e59439d915543929fb4f011d096",
    encrypted_parts_md5=[
        "81a4f6a400b9946fe4f58406400423f2",
        "8e9438741add7a1c211f98fcb37a73bc",
        "837026672dae8099996a69c9d66e07f9",
    ],
    encrypted_parts_sha256=[
        "26f2a6656af45ae7b8b76d532924498a4faff39d5b2b2c2b119959557a463132",
        "62c298fd987a6bac2066e4dbed274879247b3edd816c8351dc22ada6d37b24b0",
        "45cccbdfc4bfe2aa7f17428a087282d71be917ef059cac15a161284340840957",
    ],
)

# This file data represents an object *before* it has been registered.
#  Note the bucket ID
EXAMPLE_PENDING_FILE = PendingFileUpload(**EXAMPLE_ARCHIVABLE_FILE.model_dump())

# This file data represents an object *before* it has been completely registered.
#  Note the bucket ID
EXAMPLE_ACCESSIONED_FILE = AccessionedFileUpload(
    accession="GHGA001",
    **EXAMPLE_PENDING_FILE.model_dump(),
)
