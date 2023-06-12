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
from fis.core.ingest import UploadMetadataProcessorPort
from fis.core.models import FileUploadMetadataEncrypted

router = APIRouter()


@router.post(
    "/ingest",
    summary="Processes encrypted output data from the S3 upload script and ingests it "
    + "into the Encryption Key Store, Internal File Registry and Download Controller.",
    operation_id="ingestFileUploadMetadata",
    tags=["FileIngestService"],
    status_code=status.HTTP_202_ACCEPTED,
    response_description="Received and decrypted data successfully.",
)
@inject
async def ingest_file_upload_metadata(
    encrypted_payload: FileUploadMetadataEncrypted,
    upload_metadata_processor: UploadMetadataProcessorPort = Depends(
        Provide[Container.upload_metadata_processor]
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
        _ = await upload_metadata_processor.store_secret(file_secret=file_secret)
    except upload_metadata_processor.VaultCommunicationError as error:
        raise HTTPException(status_code=500, detail=str(error)) from error

    return Response(status_code=202)
