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

"""Defines dataclasses for holding business-logic data"""

from ghga_event_schemas import pydantic_ as event_schemas
from ghga_service_commons.utils.utc_dates import UTCDatetime
from pydantic import UUID4, BaseModel, Field


class FileUploadBasics(BaseModel):
    """Holds the fields of a FileUpload that are known before S3 upload initiation."""

    id: UUID4
    box_id: UUID4
    alias: str
    storage_alias: str
    bucket_id: str
    object_id: UUID4
    decrypted_size: int
    encrypted_size: int
    part_size: int


class FileUploadBox(event_schemas.FileUploadBox):
    """A class representing a box that bundles files belonging to the same upload."""


class ResearchDataUploadBox(event_schemas.ResearchDataUploadBox):
    """A class representing a ResearchDataUploadBox."""


class FileUpload(event_schemas.FileUpload):
    """A FileUpload.

    Contains all information required for a file's journey from upload initiation to
    permanent archival.
    """

    # Note: The following are UCS-only fields
    inbox_upload_completed: bool = Field(
        default=False,
        description="Indicates whether the file has been completely uploaded to the inbox.",
    )
    s3_upload_id: str = Field(
        default=..., description="The ID of the S3 multipart upload"
    )
    initiated: UTCDatetime = Field(
        default=..., description="When the S3 multipart upload was initiated"
    )
    completed: UTCDatetime | None = Field(
        default=None, description="When the S3 multipart upload was completed"
    )
