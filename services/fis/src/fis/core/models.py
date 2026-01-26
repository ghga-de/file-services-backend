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
"""Models for internal representation"""

from typing import Literal

from ghga_service_commons.utils.utc_dates import UTCDatetime
from pydantic import UUID4, BaseModel, Field, SecretBytes, model_validator


class FileIdModel(BaseModel):
    """Model for a file ID"""

    file_id: str


FileUploadState = Literal[
    "init",
    "inbox",
    "failed",
    "interrogated",
    "awaiting_archival",
    "archived",
]


class BaseFileInformation(BaseModel):
    """Basic file information - all that is needed for interrogation."""

    id: UUID4 = Field(..., description="Unique identifier for the file upload")
    data_hub: str = Field(..., description="The data hub at which the file is stored")
    storage_alias: str = Field(
        ..., description="The storage alias for the inbox bucket"
    )
    decrypted_sha256: str = Field(
        ..., description="SHA-256 checksum of the entire unencrypted file content"
    )
    decrypted_size: int = Field(..., description="The size of the unencrypted file")
    encrypted_size: int = Field(
        ..., description="The encrypted size of the file before re-encryption"
    )
    part_size: int = Field(
        ...,
        description="The number of bytes in each file part (last part is likely smaller)",
    )


class FileUnderInterrogation(BaseFileInformation):
    """A user-submitted file upload that needs to be interrogated"""

    id: UUID4 = Field(..., description="Unique identifier for the file upload")
    state: FileUploadState = Field(
        default="init", description="The state of the FileUpload"
    )
    state_updated: UTCDatetime = Field(
        ..., description="Timestamp of when state was updated"
    )
    interrogated: bool = Field(
        default=False, description="Indicates whether interrogation has been completed"
    )
    can_remove: bool = Field(
        default=False,
        description="Indicates whether the file can be deleted from the interrogation bucket",
    )


class InterrogationSuccess(BaseModel):
    """Event model informing services that file interrogation succeeded"""

    file_id: UUID4 = Field(..., description="Unique identifier for the file upload")
    secret_id: str | None = Field(
        default=None,
        description="The internal ID of the Data Hub-generated decryption secret",
    )
    storage_alias: str = Field(
        ..., description="The storage alias of the interrogation bucket"
    )
    interrogated_at: UTCDatetime = Field(
        ..., description="Time that the report was generated"
    )
    encrypted_parts_md5: list[str] = Field(
        ..., description="The MD5 checksum for each file part, in sequence"
    )
    encrypted_parts_sha256: list[str] = Field(
        ..., description="The SHA256 checksum for each file part, in sequence"
    )


class InterrogationFailure(BaseModel):
    """Event model informing services that file interrogation failed"""

    file_id: UUID4 = Field(..., description="Unique identifier for the file upload")
    storage_alias: str = Field(
        ..., description="The interrogation bucket storage alias"
    )
    interrogated_at: UTCDatetime = Field(
        ..., description="Time that the report was generated"
    )
    reason: str = Field(
        ..., description="The text of the error that caused interrogation to fail"
    )


class InterrogationReport(BaseModel):
    """Contains the results of file interrogation"""

    file_id: UUID4 = Field(..., description="Unique identifier for the file upload")
    storage_alias: str = Field(
        ..., description="The storage alias for the interrogation bucket"
    )
    interrogated_at: UTCDatetime = Field(
        ..., description="Timestamp showing when interrogation finished"
    )
    passed: bool = Field(..., description="Whether the interrogation was a success")
    secret: SecretBytes | None = Field(
        default=None, description="Encrypted file encryption secret"
    )
    encrypted_parts_md5: list[str] | None = Field(
        default=None, description="Conditional upon success"
    )
    encrypted_parts_sha256: list[str] | None = Field(
        default=None, description="Conditional upon success"
    )
    reason: str | None = Field(
        default=None,
        description="Conditional upon failure, contains reason for failure",
    )

    @model_validator(mode="after")
    def validate_conditional_fields(self) -> "InterrogationReport":
        """Validate that conditional fields are set based on passed status."""
        if self.passed:
            if self.encrypted_parts_md5 is None:
                raise ValueError(
                    "encrypted_parts_md5 must not be None when passed is True"
                )
            if self.encrypted_parts_sha256 is None:
                raise ValueError(
                    "encrypted_parts_sha256 must not be None when passed is True"
                )
            if self.secret is None:
                raise ValueError("secret must not be None when passed is True")
        elif self.reason is None:
            raise ValueError("reason must not be None when passed is False")
        return self

    # TODO: Write a unit test for this validator
