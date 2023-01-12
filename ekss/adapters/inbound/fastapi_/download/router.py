# Copyright 2021 - 2023 Universität Tübingen, DKFZ and EMBL
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
"""Contains routes and associated data for the download path"""

import base64

from fastapi import APIRouter, Depends, status

from ekss.adapters.inbound.fastapi_.deps import get_vault
from ekss.adapters.inbound.fastapi_.download import exceptions, models
from ekss.adapters.outbound.vault import SecretRetrievalError, VaultAdapter
from ekss.core.envelope_encryption import get_envelope

download_router = APIRouter(tags=["EncryptionKeyStoreService"])

ERROR_RESPONSES = {
    "secretNotFoundError": {
        "description": (""),
        "model": exceptions.HttpSecretNotFoundError.get_body_model(),
    },
}


@download_router.get(
    "/secrets/{secret_id}/envelopes/{client_pk}",
    summary="Get personalized envelope containing Crypt4GH file encryption/decryption key",
    operation_id="getEncryptionData",
    status_code=status.HTTP_200_OK,
    response_model=models.OutboundEnvelopeContent,
    response_description="",
    responses={
        status.HTTP_404_NOT_FOUND: ERROR_RESPONSES["secretNotFoundError"],
    },
)
async def get_header_envelope(
    *, secret_id: str, client_pk: str, vault: VaultAdapter = Depends(get_vault)
):
    """Create header envelope for the file secret with given ID encrypted with a given public key"""
    try:
        header_envelope = await get_envelope(
            secret_id=secret_id,
            client_pubkey=base64.urlsafe_b64decode(client_pk),
            vault=vault,
        )
    except SecretRetrievalError as error:
        raise exceptions.HttpSecretNotFoundError() from error

    return {
        "content": base64.b64encode(header_envelope).decode("utf-8"),
    }
