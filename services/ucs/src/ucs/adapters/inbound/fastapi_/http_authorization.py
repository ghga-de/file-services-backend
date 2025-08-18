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

"""Authorization specific code for FastAPI"""

from typing import Annotated

from fastapi import Depends, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from ghga_service_commons.auth.context import AuthContextProtocol
from ghga_service_commons.auth.policies import require_auth_context_using_credentials

from ucs.adapters.inbound.fastapi_ import dummies
from ucs.adapters.inbound.fastapi_ import rest_models as models

__all__ = [
    "require_change_file_box_work_order",
    "require_create_file_box_work_order",
    "require_create_file_work_order",
    "require_upload_file_work_order",
    "require_view_file_box_work_order",
]


async def _require_create_file_box_work_order(
    auth_provider: Annotated[
        AuthContextProtocol[models.CreateFileBoxWorkOrder],
        Depends(dummies.auth_provider),
    ],
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=True)),
) -> models.CreateFileBoxWorkOrder:
    """Require a work order context using FastAPI."""
    return await require_auth_context_using_credentials(
        credentials=credentials, auth_provider=auth_provider
    )


async def _require_change_file_box_work_order(
    auth_provider: Annotated[
        AuthContextProtocol[models.ChangeFileBoxWorkOrder],
        Depends(dummies.auth_provider),
    ],
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=True)),
) -> models.ChangeFileBoxWorkOrder:
    """Require a work order context using FastAPI."""
    return await require_auth_context_using_credentials(
        credentials=credentials, auth_provider=auth_provider
    )


async def _require_view_file_box_work_order(
    auth_provider: Annotated[
        AuthContextProtocol[models.ViewFileBoxWorkOrder], Depends(dummies.auth_provider)
    ],
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=True)),
) -> models.ViewFileBoxWorkOrder:
    """Require a work order context using FastAPI."""
    return await require_auth_context_using_credentials(
        credentials=credentials, auth_provider=auth_provider
    )


async def _require_create_file_work_order(
    auth_provider: Annotated[
        AuthContextProtocol[models.CreateFileWorkOrder], Depends(dummies.auth_provider)
    ],
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=True)),
) -> models.CreateFileWorkOrder:
    """Require a work order context using FastAPI."""
    return await require_auth_context_using_credentials(
        credentials=credentials, auth_provider=auth_provider
    )


async def _require_upload_file_work_order(
    auth_provider: Annotated[
        AuthContextProtocol[models.UploadFileWorkOrder], Depends(dummies.auth_provider)
    ],
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=True)),
) -> models.UploadFileWorkOrder:
    """Require a work order context using FastAPI."""
    return await require_auth_context_using_credentials(
        credentials=credentials, auth_provider=auth_provider
    )


require_create_file_box_work_order = Security(models.CreateFileBoxWorkOrder)
require_change_file_box_work_order = Security(models.ChangeFileBoxWorkOrder)
require_view_file_box_work_order = Security(models.ViewFileBoxWorkOrder)
require_create_file_work_order = Security(models.CreateFileWorkOrder)
require_upload_file_work_order = Security(models.UploadFileWorkOrder)
