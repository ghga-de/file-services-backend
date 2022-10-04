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
"""Contains routes and associated data for the upload path"""

import base64
import codecs

from fastapi import APIRouter, Depends, status

from ekss.api.upload import exceptions, models
from ekss.config import CONFIG
from ekss.core.dao.mongo_db import FileSecretDao
from ekss.core.envelope_decryption import extract_envelope_content

upload_router = APIRouter()

ERROR_RESPONSES = {
    "malformedOrMissingEnvelope": {
        "description": (""),
        "model": exceptions.HttpMalformedOrMissingEnvelopeError.get_body_model(),
    },
    "envelopeDecryptionError": {
        "description": (""),
        "model": exceptions.HttpEnvelopeDecrpytionError.get_body_model(),
    },
}


async def dao_injector() -> FileSecretDao:
    """Define dao as dependency to override during testing"""
    return FileSecretDao(config=CONFIG)


@upload_router.post(
    "/secrets",
    summary="Extract file encryption/decryption secret and file content offset from enevelope",
    operation_id="postEncryptionData",
    status_code=status.HTTP_200_OK,
    response_model=models.InboundEnvelopeContent,
    response_description="",
    responses={
        status.HTTP_400_BAD_REQUEST: ERROR_RESPONSES["malformedOrMissingEnvelope"],
        status.HTTP_403_FORBIDDEN: ERROR_RESPONSES["envelopeDecryptionError"],
    },
)
async def post_encryption_secrets(
    *,
    envelope_query: models.InboundEnvelopeQuery,
    dao: FileSecretDao = Depends(dao_injector),
):
    """Extract file encryption/decryption secret, create secret ID and extract
    file content offset"""
    # Mypy false positives
    client_pubkey = base64.b64decode(
        codecs.decode(envelope_query.public_key, "hex"),
    )
    file_part = base64.b64decode(codecs.decode(envelope_query.file_part, "hex"))
    try:

        file_secret, offset = await extract_envelope_content(
            file_part=file_part,
            client_pubkey=client_pubkey,
        )
    except ValueError as error:
        # Everything in crypt4gh is an ValueError... try to distinguish based on message
        if "No supported encryption method" == str(error):
            raise exceptions.HttpEnvelopeDecrpytionError() from error
        raise exceptions.HttpMalformedOrMissingEnvelopeError() from error
    stored_secret = await dao.insert_file_secret(file_secret=file_secret)
    return {
        "secret": base64.b64encode(file_secret).hex(),
        "secret_id": stored_secret.id,
        "offset": offset,
    }
