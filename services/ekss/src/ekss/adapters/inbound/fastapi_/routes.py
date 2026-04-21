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
"""Contains routes and associated data for the upload path"""

import base64

from crypt4gh.keys import get_private_key
from fastapi import APIRouter, Depends, Request, status
from ghga_service_commons.utils.crypt import decrypt
from requests.exceptions import RequestException

from ekss.adapters.inbound.fastapi_ import exceptions, models
from ekss.adapters.inbound.fastapi_.deps import config_injector
from ekss.adapters.outbound.vault import VaultAdapter
from ekss.adapters.outbound.vault.exceptions import (
    SecretInsertionError,
    SecretRetrievalError,
)
from ekss.config import Config
from ekss.constants import TRACER
from ekss.core.envelope_encryption import get_envelope

router = APIRouter(tags=["EncryptionKeyStoreService"])

ERROR_RESPONSES = {
    "secretInsertionError": {
        "description": ("Failed to successfully insert secret into vault."),
        "model": exceptions.HttpSecretInsertionError.get_body_model(),
    },
    "vaultConnectionError": {
        "description": ("Failed to establish a connection to vault."),
        "model": exceptions.HttpVaultConnectionError.get_body_model(),
    },
    "secretNotFoundError": {
        "description": ("Could not find a secret for the given secret ID."),
        "model": exceptions.HttpSecretNotFoundError.get_body_model(),
    },
    "decodingError": {
        "description": (
            "One of the provided inputs could not be decoded as base64 string."
        ),
        "model": exceptions.HttpDecodingError.get_body_model(),
    },
}


@router.get(
    "/health",
    summary="health",
    status_code=status.HTTP_200_OK,
)
@TRACER.start_as_current_span("routes.health")
async def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@router.post(
    "/secrets",
    summary="Store Crypt4GH-encrypted file encryption secret",
    operation_id="postEncryptionData",
    status_code=status.HTTP_200_OK,
    response_model=models.SecretID,
    response_description="Successfully stored file encryption secret.",
    responses={
        status.HTTP_422_UNPROCESSABLE_ENTITY: ERROR_RESPONSES["decodingError"],
        status.HTTP_502_BAD_GATEWAY: ERROR_RESPONSES["secretInsertionError"],
        status.HTTP_504_GATEWAY_TIMEOUT: ERROR_RESPONSES["vaultConnectionError"],
    },
)
@TRACER.start_as_current_span("routes.post_encryption_secret")
async def post_encryption_secret(
    *,
    request: Request,
    config: Config = Depends(config_injector),
):
    """Extract file encryption/decryption secret, create secret ID and extract
    file content offset
    """
    vault = VaultAdapter(config)
    try:
        encrypted_secret = (await request.body()).decode("utf-8")
    except Exception as error:
        raise exceptions.HttpDecodingError(affected="encrypted secret") from error

    server_private_key = get_private_key(
        config.server_private_key_path, lambda: config.private_key_passphrase
    )
    file_secret = decrypt(encrypted_secret, server_private_key)

    try:
        secret_id = vault.store_secret(secret=file_secret.encode())
    except SecretInsertionError as error:
        raise exceptions.HttpSecretInsertionError() from error
    except RequestException as error:
        raise exceptions.HttpVaultConnectionError() from error

    return {"secret_id": secret_id}


@router.get(
    "/secrets/{secret_id}/envelopes/{client_pk}",
    summary="Get personalized envelope containing Crypt4GH file encryption/decryption key",
    operation_id="getEncryptionData",
    status_code=status.HTTP_200_OK,
    response_model=models.OutboundEnvelopeContent,
    response_description="Created personalized Crypt4GH envelope.",
    responses={
        status.HTTP_404_NOT_FOUND: ERROR_RESPONSES["secretNotFoundError"],
        status.HTTP_422_UNPROCESSABLE_ENTITY: ERROR_RESPONSES["decodingError"],
    },
)
@TRACER.start_as_current_span("routes.get_header_envelope")
async def get_header_envelope(
    *, secret_id: str, client_pk: str, config: Config = Depends(config_injector)
):
    """Create header envelope for the file secret with given ID encrypted with a given public key"""
    vault = VaultAdapter(config)
    try:
        client_pubkey = base64.urlsafe_b64decode(client_pk)
    except Exception as error:
        raise exceptions.HttpDecodingError(affected="client public key") from error
    try:
        header_envelope = await get_envelope(
            secret_id=secret_id,
            client_pubkey=client_pubkey,
            server_private_key_path=config.server_private_key_path,
            passphrase=config.private_key_passphrase,
            vault=vault,
        )
    except SecretRetrievalError as error:
        raise exceptions.HttpSecretNotFoundError() from error

    return {
        "content": base64.b64encode(header_envelope).decode("utf-8"),
    }


@router.delete(
    "/secrets/{secret_id}",
    summary="Delete the associated secret",
    operation_id="deleteSecret",
    status_code=status.HTTP_204_NO_CONTENT,
    response_description="Successfully deleted secret.",
    responses={
        status.HTTP_404_NOT_FOUND: ERROR_RESPONSES["secretNotFoundError"],
    },
)
@TRACER.start_as_current_span("route.delete_secret")
async def delete_secret(*, secret_id: str, config: Config = Depends(config_injector)):
    """Create header envelope for the file secret with given ID encrypted with a given public key"""
    vault = VaultAdapter(config)
    try:
        vault.delete_secret(key=secret_id)
    except SecretRetrievalError as error:
        raise exceptions.HttpSecretNotFoundError() from error

    return status.HTTP_204_NO_CONTENT
