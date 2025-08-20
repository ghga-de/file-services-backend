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

from enum import StrEnum
from typing import Literal

from pydantic import UUID4, BaseModel, ConfigDict, Field, field_validator


class BoxCreationRequest(BaseModel):
    """Request body for creating a new FileUploadBox."""

    research_data_upload_box_id: UUID4 = Field(
        ..., description="The ID of the corresponding ResearchDataUploadBox"
    )
    storage_alias: str = Field(
        ..., description="The storage alias to use for this upload box"
    )
    model_config = ConfigDict(title="Box Creation Request")


class BoxUpdateRequest(BaseModel):
    """Request body for updating a FileUploadBox."""

    locked: bool = Field(
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


class UploadAttemptCreation(BaseModel):
    """Properties required to create a new upload."""

    file_id: str = Field(
        ..., description="The ID of the file corresponding to this upload."
    )
    submitter_public_key: str = Field(
        ..., description="The public key used by the submittter to encrypt the file."
    )
    storage_alias: str = Field(
        ...,
        description="Alias identifying the object storage location to use for this upload",
    )
    model_config = ConfigDict(title="Properties required to create a new upload")


class UploadAttemptUpdate(BaseModel):
    """Request body to update an existing mutli-part upload."""

    status: Literal["uploaded", "cancelled"]
    model_config = ConfigDict(title="Multi-Part Upload Update")


class WorkType(StrEnum):
    """The type of work that a work package describes."""

    CREATE = "create"
    LOCK = "lock"
    UNLOCK = "unlock"
    VIEW = "view"
    UPLOAD = "upload"
    CLOSE = "close"
    DELETE = "delete"


class BaseWorkOrderToken(BaseModel):
    """Base model pre-configured for use as Dto."""

    work_type: WorkType

    model_config = ConfigDict(frozen=True)


class CreateFileBoxWorkOrder(BaseWorkOrderToken):
    """WOT schema authorizing a user to create a new FileUploadBox"""

    @classmethod
    @field_validator("work_type")
    def enforce_work_type(cls, work_type):
        """Make sure work type matches expectation"""
        if work_type != WorkType.CREATE:
            raise ValueError("Work type must be 'create'.")
        return work_type


class ChangeFileBoxWorkOrder(BaseWorkOrderToken):
    """WOT schema authorizing a user to lock or unlock an existing FileUploadBox"""

    box_id: UUID4

    @classmethod
    @field_validator("work_type")
    def enforce_work_type(cls, work_type):
        """Make sure work type matches expectation"""
        if work_type not in [WorkType.LOCK, WorkType.UNLOCK]:
            raise ValueError("Work type must be 'lock' or 'unlock'.")
        return work_type


class ViewFileBoxWorkOrder(BaseWorkOrderToken):
    """WOT schema authorizing a user to view a FileUploadBox and its FileUploads"""

    box_id: UUID4

    @classmethod
    @field_validator("work_type")
    def enforce_work_type(cls, work_type):
        """Make sure work type matches expectation"""
        if work_type != WorkType.VIEW:
            raise ValueError("Work type must be 'view'.")
        return work_type


class CreateFileWorkOrder(BaseWorkOrderToken):
    """WOT schema authorizing a user to create a new FileUpload"""

    alias: str
    box_id: UUID4

    @classmethod
    @field_validator("work_type")
    def enforce_work_type(cls, work_type):
        """Make sure work type matches expectation"""
        if work_type != WorkType.CREATE:
            raise ValueError("Work type must be 'create'.")
        return work_type


class UploadFileWorkOrder(BaseWorkOrderToken):
    """WOT schema authorizing a user to work with existing FileUploads"""

    file_id: UUID4
    box_id: UUID4

    @classmethod
    @field_validator("work_type")
    def enforce_work_type(cls, work_type):
        """Make sure work type matches expectation"""
        if work_type not in [WorkType.UPLOAD, WorkType.CLOSE, WorkType.DELETE]:
            raise ValueError("Work type must be 'upload', 'close', or 'delete'.")
        return work_type
