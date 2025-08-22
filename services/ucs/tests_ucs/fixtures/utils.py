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

"""General testing utilities"""

from pathlib import Path
from typing import Literal, TypeAlias
from uuid import UUID, uuid4

from ghga_service_commons.utils import jwt_helpers
from jwcrypto.jwk import JWK
from pydantic import UUID4

from ucs.adapters.inbound.fastapi_ import rest_models as models

BASE_DIR = Path(__file__).parent.resolve()

SignedToken: TypeAlias = str


def null_func(*args, **kwargs):
    """I accept any args and kwargs but I do nothing."""
    pass


def is_success_http_code(http_code: int) -> bool:
    """Checks if a http response code indicates success (a 2xx code)."""
    return http_code >= 200 and http_code < 300


def generate_create_file_box_token(*, jwk: JWK, valid_seconds: int = 30):
    """Generate CreateFileBoxWorkOrder token for testing."""
    wot = models.CreateFileBoxWorkOrder(work_type=models.WorkType.CREATE)
    claims = wot.model_dump(mode="json")
    signed_token = jwt_helpers.sign_and_serialize_token(
        claims=claims, key=jwk, valid_seconds=valid_seconds
    )
    return signed_token


def generate_change_file_box_token(
    *,
    box_id: UUID = uuid4(),
    work_type: Literal["lock"] | Literal["unlock"] = "lock",
    jwk: JWK,
    valid_seconds: int = 30,
):
    """Generate ChangeFileBoxWorkOrder token for testing.

    Leave box_id unspecified to use a random value.
    """
    wot = models.ChangeFileBoxWorkOrder(work_type=work_type, box_id=box_id)
    claims = wot.model_dump(mode="json")
    signed_token = jwt_helpers.sign_and_serialize_token(
        claims=claims, key=jwk, valid_seconds=valid_seconds
    )
    return signed_token


def generate_view_file_box_token(
    *, box_id: UUID = uuid4(), jwk: JWK, valid_seconds: int = 30
):
    """Generate ViewFileBoxWorkOrder token for testing.

    Leave box_id unspecified to use a random value.
    """
    wot = models.ViewFileBoxWorkOrder(work_type=models.WorkType.VIEW, box_id=box_id)
    claims = wot.model_dump(mode="json")
    signed_token = jwt_helpers.sign_and_serialize_token(
        claims=claims, key=jwk, valid_seconds=valid_seconds
    )
    return signed_token


def generate_create_file_token(
    *,
    box_id: UUID = uuid4(),
    alias: str = "junk",
    work_type: models.WorkType = models.WorkType.CREATE,
    jwk: JWK,
    valid_seconds: int = 30,
):
    """Generate CreateFileWorkOrder token for testing.

    Leave box_id and alias unspecified to use random values.
    Work_type can be specified here to test with wrong work type.
    """
    wot = models.CreateFileWorkOrder(work_type=work_type, box_id=box_id, alias=alias)
    claims = wot.model_dump(mode="json")
    signed_token = jwt_helpers.sign_and_serialize_token(
        claims=claims, key=jwk, valid_seconds=valid_seconds
    )
    return signed_token


def generate_upload_file_token(
    *,
    box_id: UUID = uuid4(),
    file_id: UUID4 = uuid4(),
    jwk: JWK,
    valid_seconds: int = 30,
):
    """Generate UploadFileWorkOrder token with type UPLOAD for testing.

    Leave box_id and file_id unspecified to use random values.
    """
    wot = models.UploadFileWorkOrder(
        work_type=models.WorkType.UPLOAD, box_id=box_id, file_id=file_id
    )
    claims = wot.model_dump(mode="json")
    signed_token = jwt_helpers.sign_and_serialize_token(
        claims=claims, key=jwk, valid_seconds=valid_seconds
    )
    return signed_token


def generate_close_file_token(
    *,
    box_id: UUID = uuid4(),
    file_id: UUID4 = uuid4(),
    jwk: JWK,
    valid_seconds: int = 30,
):
    """Generate UploadFileWorkOrder token with type CLOSE for testing.

    Leave box_id and file_id unspecified to use random values.
    """
    wot = models.UploadFileWorkOrder(
        work_type=models.WorkType.CLOSE, box_id=box_id, file_id=file_id
    )
    claims = wot.model_dump(mode="json")
    signed_token = jwt_helpers.sign_and_serialize_token(
        claims=claims, key=jwk, valid_seconds=valid_seconds
    )
    return signed_token


def generate_delete_file_token(
    *,
    box_id: UUID = uuid4(),
    file_id: UUID4 = uuid4(),
    jwk: JWK,
    valid_seconds: int = 30,
):
    """Generate UploadFileWorkOrder token with type DELETE for testing.

    Leave box_id and file_id unspecified to use random values.
    """
    wot = models.UploadFileWorkOrder(
        work_type=models.WorkType.DELETE, box_id=box_id, file_id=file_id
    )
    claims = wot.model_dump(mode="json")
    signed_token = jwt_helpers.sign_and_serialize_token(
        claims=claims, key=jwk, valid_seconds=valid_seconds
    )
    return signed_token


def generate_token_signing_keys() -> JWK:
    """Generate JWK credentials that can be used for signing and verification
    of JWT tokens.
    """
    return jwt_helpers.generate_jwk()
