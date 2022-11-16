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
"""Defines exceptions that can occur during envelope data extraction"""

from httpyexpect.server import HttpCustomExceptionBase
from pydantic import BaseModel


class HttpMalformedOrMissingEnvelopeError(HttpCustomExceptionBase):
    """Thrown when envelope decryption fails due to a missing or malformed envelope."""

    exception_id = "malformedOrMissingEnvelopeError"

    class DataModel(BaseModel):
        """Model for exception data"""

    def __init__(self, *, status_code: int = 400):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description=("Envelope malformed or missing"),
            data={},
        )


class HttpEnvelopeDecryptionError(HttpCustomExceptionBase):
    """Thrown when no available secret crypt4GH key can successfully decrypt the file envelope."""

    exception_id = "envelopeDecryptionError"

    class DataModel(BaseModel):
        """Model for exception data"""

    def __init__(self, *, status_code: int = 403):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description=("Could not decrypt envelope content with given keys"),
            data={},
        )
