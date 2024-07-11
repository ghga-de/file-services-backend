# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

from typing import Annotated

from fastapi import APIRouter, HTTPException, Response, status

from fins.adapters.inbound.fastapi_ import dummies

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


@router.get("file_information/{file_id}",
            summary="",
            operation_id="getFileInformation",
            tags=["FileIngestService"],
            status_code=status.HTTP_200_OK,
            response_description="")
async def get_file_information(file_id):
    """TODO"""
