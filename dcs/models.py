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

"""Defines dataclasses for business-logic data as well as request/reply models for use
in the api."""


import re
from datetime import datetime
from typing import List, Literal

from ghga_service_chassis_lib.object_storage_dao import (
    ObjectIdValidationError,
    validate_object_id,
)
from pydantic import UUID4, BaseModel, validator


class DrsObjectBase(BaseModel):
    """
    A model containing the metadata needed to register a new DRS object.
    """

    md5_checksum: str
    size: int
    file_id: str
    creation_date: datetime
    update_date: datetime
    format: str

    class Config:
        """Additional pydantic configs."""

        orm_mode = True

    # pylint: disable=no-self-argument,no-self-use
    @validator("file_id")
    def check_file_id(cls, value: str):
        """Checks if the file_id is valid for use as a s3 object id."""

        try:
            validate_object_id(value)
        except ObjectIdValidationError as error:
            raise ValueError(
                f"File ID '{value}' cannot be used as a (S3) object id."
            ) from error

        return value


class DrsObjectComplete(DrsObjectBase):
    """
    A model for describing all DrsObject metadata.
    Only intended for service-internal use.
    """

    id: UUID4


class AccessURL(BaseModel):
    """Describes the URL for accessing the actual bytes of the object as per the
    DRS OpenApi spec."""

    url: str


class AccessMethod(BaseModel):
    """A AccessMethod as per the DRS OpenApi spec."""

    access_url: AccessURL
    type: Literal["s3"] = "s3"  # currently only s3 is supported


class Checksum(BaseModel):
    """
    A Checksum as per the DRS OpenApi specs.
    """

    checksum: str
    type: Literal["md5", "sha-256"]


class DrsObjectServe(BaseModel):
    """
    A model containing a DrsObject as per the DRS OpenApi specs.
    This is used to serve metadata on a DrsObject (including the access methods) to the
    user.
    """

    file_id: str  # the file ID
    self_uri: str
    size: int
    created_time: str
    updated_time: str
    checksums: List[Checksum]
    access_methods: List[AccessMethod]

    # pylint: disable=no-self-argument,no-self-use
    @validator("self_uri")
    def check_self_uri(cls, value: str):
        """Checks if the self_uri is a valid DRS URI."""

        if not re.match(r"^drs://.+\..+/.+", value):
            ValueError(f"The self_uri '{value}' is no valid DRS URI.")

        return value
