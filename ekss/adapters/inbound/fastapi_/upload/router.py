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
"""Contains routes and associated data for the upload path"""

import base64
import os

from fastapi import APIRouter, Depends, status
from requests.exceptions import RequestException

from ekss.adapters.inbound.fastapi_.deps import get_vault
from ekss.adapters.inbound.fastapi_.upload import exceptions, models
from ekss.adapters.outbound.vault import VaultAdapter
from ekss.adapters.outbound.vault.exceptions import SecretInsertionError
from ekss.core.envelope_decryption import extract_envelope_content

upload_router = APIRouter(tags=["EncryptionKeyStoreService"])
ERROR_RESPONSES = {
    "malformedOrMissingEnvelope": {
        "description": (""),
        "model": exceptions.HttpMalformedOrMissingEnvelopeError.get_body_model(),
    },
    "envelopeDecryptionError": {
        "description": (""),
        "model": exceptions.HttpEnvelopeDecryptionError.get_body_model(),
    },
    "secretInsertionError": {
        "description": (""),
        "model": exceptions.HttpSecretInsertionError.get_body_model(),
    },
    "vaultConnectionError": {
        "description": (""),
        "model": exceptions.HttpVaultConnectionError.get_body_model(),
    },
}


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
        status.HTTP_502_BAD_GATEWAY: ERROR_RESPONSES["secretInsertionError"],
        status.HTTP_504_GATEWAY_TIMEOUT: ERROR_RESPONSES["vaultConnectionError"],
    },
)
async def post_encryption_secrets(
    *,
    envelope_query: models.InboundEnvelopeQuery,
    vault: VaultAdapter = Depends(get_vault),
):
    """Extract file encryption/decryption secret, create secret ID and extract
    file content offset"""
    client_pubkey = base64.b64decode(envelope_query.public_key)
    file_part = base64.b64decode(envelope_query.file_part)
    try:

        submitter_secret, offset = await extract_envelope_content(
            file_part=file_part,
            client_pubkey=client_pubkey,
        )
    except ValueError as error:
        # Everything in envelope decryption is a ValueError... try to distinguish based on message
        if "No supported encryption method" == str(error):
            raise exceptions.HttpEnvelopeDecryptionError() from error
        raise exceptions.HttpMalformedOrMissingEnvelopeError() from error

    # generate a new secret for re-encryption
    new_secret = os.urandom(32)
    try:
        secret_id = vault.store_secret(secret=new_secret)
    except SecretInsertionError as error:
        raise exceptions.HttpSecretInsertionError() from error
    except RequestException as error:
        raise exceptions.HttpVaultConnectionError() from error

    return {
        "submitter_secret": base64.b64encode(submitter_secret).decode("utf-8"),
        "new_secret": base64.b64encode(new_secret).decode("utf-8"),
        "secret_id": secret_id,
        "offset": offset,
    }
