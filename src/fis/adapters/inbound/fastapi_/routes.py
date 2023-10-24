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
"""FastAPI routes for S3 upload metadata ingest"""

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException, Response, status

from fis.adapters.inbound.fastapi_.http_authorization import (
    IngestTokenAuthContext,
    require_token,
)
from fis.container import Container
from fis.core.ingest import LegacyUploadMetadataProcessor, UploadMetadataProcessor
from fis.core.models import EncryptedPayload

router = APIRouter()


@router.get(
    "/health",
    summary="health",
    tags=["FileIngestService"],
    status_code=200,
)
async def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@router.post(
    "/legacy/ingest",
    summary="Processes encrypted output data from the S3 upload script and ingests it "
    + "into the Encryption Key Store, Internal File Registry and Download Controller.",
    operation_id="ingestLegacyFileUploadMetadata",
    tags=["FileIngestService"],
    status_code=status.HTTP_202_ACCEPTED,
    response_description="Received and decrypted data successfully.",
    deprecated=True,
)
@inject
async def ingest_legacy_metadata(
    encrypted_payload: EncryptedPayload,
    upload_metadata_processor: LegacyUploadMetadataProcessor = Depends(
        Provide[Container.legacy_upload_metadata_processor]
    ),
    _token: IngestTokenAuthContext = require_token,
):
    """Decrypt payload, process metadata, file secret and send success event"""
    try:
        decrypted_metadata = await upload_metadata_processor.decrypt_payload(
            encrypted=encrypted_payload
        )
    except (
        upload_metadata_processor.DecryptionError,
        upload_metadata_processor.WrongDecryptedFormatError,
    ) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error

    file_secret = decrypted_metadata.file_secret

    try:
        secret_id = await upload_metadata_processor.store_secret(
            file_secret=file_secret
        )
    except upload_metadata_processor.VaultCommunicationError as error:
        raise HTTPException(status_code=500, detail=str(error)) from error

    await upload_metadata_processor.populate_by_event(
        upload_metadata=decrypted_metadata, secret_id=secret_id
    )

    return Response(status_code=202)


@router.post(
    "/federated/ingest_metadata",
    summary="Processes encrypted output data from the S3 upload script and ingests it "
    + "into the Encryption Key Store, Internal File Registry and Download Controller.",
    operation_id="ingestFileUploadMetadata",
    tags=["FileIngestService"],
    status_code=status.HTTP_202_ACCEPTED,
    response_description="Received and decrypted data successfully.",
)
@inject
async def ingest_metadata(
    encrypted_payload: EncryptedPayload,
    upload_metadata_processor: UploadMetadataProcessor = Depends(
        Provide[Container.upload_metadata_processor]
    ),
    _token: IngestTokenAuthContext = require_token,
):
    """Decrypt payload, process metadata, file secret id and send success event"""
    try:
        decrypted_metadata = await upload_metadata_processor.decrypt_payload(
            encrypted=encrypted_payload
        )
    except (
        upload_metadata_processor.DecryptionError,
        upload_metadata_processor.WrongDecryptedFormatError,
    ) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error

    secret_id = decrypted_metadata.secret_id

    await upload_metadata_processor.populate_by_event(
        upload_metadata=decrypted_metadata, secret_id=secret_id
    )

    return Response(status_code=202)


@router.post(
    "/federated/ingest_secret",
    summary="Store file encryption/decryption secret and return secret ID.",
    operation_id="ingestSecret",
    tags=["FileIngestService"],
    status_code=status.HTTP_200_OK,
    response_description="Received and stored secret successfully.",
)
@inject
async def ingest_secret(
    encrypted_payload: EncryptedPayload,
    upload_metadata_processor: UploadMetadataProcessor = Depends(
        Provide[Container.upload_metadata_processor]
    ),
    _token: IngestTokenAuthContext = require_token,
):
    """Decrypt payload and deposit file secret in exchange for a secret id"""
    file_secret = await upload_metadata_processor.decrypt_secret(
        encrypted=encrypted_payload
    )

    secret_id = await upload_metadata_processor.store_secret(file_secret=file_secret)
    return {"secret_id": secret_id}
