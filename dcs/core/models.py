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

from pydantic import BaseModel, validator


class FileToRegister(BaseModel):
    """
    A model containing the metadata needed to register a new DRS object.
    """

    file_id: str
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

    # pylint: disable=no-self-argument,no-self-use
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
