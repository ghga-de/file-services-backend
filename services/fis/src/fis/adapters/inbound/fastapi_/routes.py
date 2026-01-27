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
"""FastAPI routes for S3 upload metadata ingest"""

import logging
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, status
from pydantic import UUID4

from fis.adapters.inbound.fastapi_ import dummies
from fis.adapters.inbound.fastapi_.http_authorization import JWT, require_data_hub_jwt
from fis.constants import TRACER
from fis.core import models
from fis.ports.inbound.interrogation import InterrogationHandlerPort

log = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/health",
    summary="health",
    tags=["FileIngestService"],
    status_code=200,
)
@TRACER.start_as_current_span("routes.health")
async def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@router.get(
    "/hubs/{data_hub}/uploads",
    summary="Serve a list of new file uploads (yet to be interrogated)",
    operation_id="listUploads",
    tags=["FileIngestService"],
    status_code=status.HTTP_200_OK,
)
@TRACER.start_as_current_span("routes.list_uploads")
async def list_uploads(
    data_hub: str,
    interrogator: dummies.InterrogatorPort,
    _token: Annotated[JWT, require_data_hub_jwt],
) -> list[models.BaseFileInformation]:
    """Return a list of not-yet-interrogated files for a Data Hub"""
    try:
        return await interrogator.get_files_not_yet_interrogated(data_hub=data_hub)
    except Exception as err:
        error = HTTPException(status_code=500, detail="Something went wrong.")
        log.error(error, exc_info=True)
        raise error from err


@router.post(
    "/hubs/{data_hub}/uploads/can_remove",
    summary="Returns a list of IDs indicating which files can be removed from the interrogation bucket",
    operation_id="getRemovableFiles",
    tags=["FileIngestService"],
    status_code=status.HTTP_200_OK,
)
@TRACER.start_as_current_span("routes.get_removable_files")
async def get_removable_files(
    data_hub: str,
    interrogator: dummies.InterrogatorPort,
    _token: Annotated[JWT, require_data_hub_jwt],
    file_ids: list[UUID4] = Body(),
) -> list[UUID4]:
    """Returns a subset of the provided file ID list containing the IDs of all files
    which may be now removed from the interrogation bucket.
    """
    try:
        return [f for f in file_ids if await interrogator.check_if_removable(file_id=f)]
    except Exception as err:
        error = HTTPException(status_code=500, detail="Something went wrong.")
        log.error(error, exc_info=True)
        raise error from err


@router.post(
    "/hubs/{data_hub}/interrogation-reports",
    summary="Accepts an InterrogationReport for a file",
    operation_id="postInterrogationReport",
    tags=["FileIngestService"],
    status_code=status.HTTP_201_CREATED,
    responses={status.HTTP_404_NOT_FOUND: {"description": "No such file exists"}},
)
@TRACER.start_as_current_span("routes.get_removable_files")
async def post_interrogation_report(
    data_hub: str,
    interrogator: dummies.InterrogatorPort,
    _token: Annotated[JWT, require_data_hub_jwt],
    report: models.InterrogationReport = Body(),
) -> None:
    """Post an InterrogationReport"""
    try:
        await interrogator.handle_interrogation_report(report=report)
    except InterrogationHandlerPort.FileNotFoundError as err:
        raise HTTPException(
            status_code=404, detail=f"File {report.file_id} not found"
        ) from err
    except Exception as err:
        error = HTTPException(status_code=500, detail="Something went wrong.")
        log.error(error, exc_info=True, extra={"file_id": report.file_id})
        raise error from err
