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


"""
Module containing the main FastAPI router and all route functions.
"""

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from dcs.adapters.inbound.fastapi_ import http_exceptions, http_responses
from dcs.container import Container
from dcs.core.models import DrsObjectWithAccess
from dcs.ports.inbound.data_repository import DataRepositoryPort

router = APIRouter()


class DeliveryDelayedModel(BaseModel):
    """Pydantic model for 202 Response. Empty, since 202 has no body."""


RESPONSES = {
    "noSuchObject": {
        "description": (
            "Exceptions by ID:"
            + "\n- noSuchUpload: The requested DrsObject wasn't found"
        ),
        "model": http_exceptions.HttpObjectNotFoundError.get_body_model(),
    },
    "objectNotInOutbox": {
        "description": (
            "The operation is delayed and will continue asynchronously. "
            + "The client should retry this same request after the delay "
            + "specified by Retry-After header."
        ),
        "model": DeliveryDelayedModel,
    },
}


@router.get("/health", summary="health", status_code=status.HTTP_200_OK)
async def health():
    """Used to test if this service is alive"""

    return {"status": "OK"}


@router.get(
    "/objects/{object_id}",
    summary="Returns object metadata, and a list of access methods that can be used "
    + "to fetch object bytes.",
    operation_id="getDrsObject",
    status_code=status.HTTP_200_OK,
    response_model=DrsObjectWithAccess,
    response_description="The DrsObject was found successfully.",
    responses={
        status.HTTP_202_ACCEPTED: RESPONSES["objectNotInOutbox"],
        status.HTTP_404_NOT_FOUND: RESPONSES["noSuchObject"],
    },
)
@inject
async def get_drs_object(
    object_id: str,
    data_repository: DataRepositoryPort = Depends(Provide[Container.data_repository]),
):
    """
    Get info about a ``DrsObject``.
    """

    try:
        drs_object = await data_repository.access_drs_object(drs_id=object_id)
        return drs_object

    except data_repository.RetryAccessLaterError as retry_later_error:
        # tell client to retry after 5 minutes
        return http_responses.HttpObjectNotInOutboxResponse(
            retry_after=retry_later_error.retry_after
        )

    except data_repository.DrsObjectNotFoundError as object_not_found_error:
        raise http_exceptions.HttpObjectNotFoundError(
            object_id=object_id
        ) from object_not_found_error
