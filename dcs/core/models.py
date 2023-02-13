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

"""Defines dataclasses for business-logic data as well as request/reply models for use
in the api."""


import re
from datetime import datetime

from pydantic import BaseModel, validator


class Download(BaseModel):
    """Model for ongoing downloads"""

    id: str
    file_id: str
    envelope_id: str
    signature_hash: str
    # lifetime should expire 30s after creation
    expiration_datetime: str

    @validator("expiration_datetime")
    @classmethod
    def check_datetime_format(cls, expiration_datetime):
        """Ensure provided date string can be interpreted as datetime"""
        return validated_date(expiration_datetime)


class Envelope(BaseModel):
    """Model caching envelope for ongoing download"""

    # hash(object_id + pubkey)
    id: str
    header: bytes
    offset: int
    creation_timestamp: str

    @validator("creation_timestamp")
    @classmethod
    def check_datetime_format(cls, creation_timestamp):
        """Ensure provided date string can be interpreted as datetime"""
        return validated_date(creation_timestamp)


class FileToRegister(BaseModel):
    """
    A model containing the metadata needed to register a new DRS object.
    """

    file_id: str
    decryption_secret_id: str
    decrypted_sha256: str
    decrypted_size: int
    creation_date: str


class DrsObject(FileToRegister):
    """
    A model for describing essential DRS object metadata.
    """

    id: str


class DrsObjectWithUri(DrsObject):
    """A model for describing DRS object metadata including a self URI."""

    self_uri: str

    # pylint: disable=no-self-argument
    @validator("self_uri")
    def check_self_uri(cls, value: str):
        """Checks if the self_uri is a valid DRS URI."""

        if not re.match(r"^drs://.+/.+", value):
            ValueError(f"The self_uri '{value}' is no valid DRS URI.")

        return value


class DrsObjectWithAccess(DrsObjectWithUri):
    """A model for describing DRS object metadata including information on how to access
    its content."""

    access_url: str


def validated_date(date: str):
    """Ensure that the provided string representation can be interpreted as a datetime"""
    try:
        datetime.fromisoformat(date)
    except ValueError as exc:
        raise ValueError(
            f"Could not convert provided string to datetime: {date}"
        ) from exc
    return date
