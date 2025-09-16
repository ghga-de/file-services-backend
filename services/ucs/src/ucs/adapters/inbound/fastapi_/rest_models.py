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

"""REST API-specific data models (not used by core package)"""

from typing import Literal, TypeVar

from pydantic import UUID4, BaseModel, ConfigDict, Field


class BoxCreationRequest(BaseModel):
    """Request body for creating a new FileUploadBox."""

    storage_alias: str = Field(
        ..., description="The storage alias to use for this upload box"
    )
    model_config = ConfigDict(title="Box Creation Request")


class BoxUpdateRequest(BaseModel):
    """Request body for updating a FileUploadBox."""

    lock: bool = Field(
        ..., description="Whether the box should be locked (true) or unlocked (false)"
    )
    model_config = ConfigDict(title="Box Update Request")


class BoxUploadsResponse(BaseModel):
    """Response body for listing file IDs in a FileUploadBox."""

    file_ids: list[UUID4] = Field(
        ..., description="List of file IDs for completed uploads in the box"
    )
    model_config = ConfigDict(title="Box Uploads Response")


class FileUploadCreationRequest(BaseModel):
    """Request body for creating a new FileUpload."""

    alias: str = Field(
        ...,
        description="The alias for the file within the box (must be unique within the box)",
    )
    checksum: str = Field(..., description="The checksum of the file")
    size: int = Field(..., description="The size of the file in bytes", ge=1)
    model_config = ConfigDict(title="File Upload Creation Request")


WorkType = Literal["create", "lock", "unlock", "view", "upload", "close", "delete"]

T = TypeVar("T", bound=WorkType)


class BaseWorkOrderToken[T: WorkType](BaseModel):
    """Base model pre-configured for use as Dto."""

    work_type: T
    model_config = ConfigDict(frozen=True)


CreateFileBoxWorkOrder = BaseWorkOrderToken[Literal["create"]]


class ChangeFileBoxWorkOrder(BaseWorkOrderToken[Literal["lock", "unlock"]]):
    """WOT schema authorizing a user to lock or unlock an existing FileUploadBox"""

    box_id: UUID4


class ViewFileBoxWorkOrder(BaseWorkOrderToken[Literal["view"]]):
    """WOT schema authorizing a user to view a FileUploadBox and its FileUploads"""

    box_id: UUID4


class CreateFileWorkOrder(BaseWorkOrderToken[Literal["create"]]):
    """WOT schema authorizing a user to create a new FileUpload"""

    alias: str
    box_id: UUID4


class UploadFileWorkOrder(BaseWorkOrderToken[Literal["upload", "close", "delete"]]):
    """WOT schema authorizing a user to work with existing FileUploads"""

    file_id: UUID4
    box_id: UUID4
