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


"""A collection of http exceptions."""

from httpyexpect.server import HttpCustomExceptionBase
from pydantic import BaseModel


class HttpObjectNotFoundError(HttpCustomExceptionBase):
    """Thrown when a file with given ID could not be found."""

    exception_id = "noSuchObject"

    class DataModel(BaseModel):
        """Model for exception data"""

        object_id: str

    def __init__(self, *, object_id: str, status_code: int = 404):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description="The requested DrsObject wasn't found",
            data={"object_id": object_id},
        )
