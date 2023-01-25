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
"""Defines exceptions that can occur during envelope creation"""

from httpyexpect.server import HttpCustomExceptionBase
from pydantic import BaseModel


class HttpSecretNotFoundError(HttpCustomExceptionBase):
    """Thrown when no secret with the given id could be found"""

    exception_id = "secretNotFoundError"

    class DataModel(BaseModel):
        """Model for exception data"""

    def __init__(self, *, status_code: int = 404):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description="The secret for the given id was not found.",
            data={},
        )
