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

"""Interface for managing and accessing DRS objects."""

from abc import ABC, abstractmethod

from dcs.core import models


class DataRepositoryPort(ABC):
    """A service that manages a registry of DRS objects."""

    class APICommunicationError(RuntimeError):
        """Raised when communication with external API fails due to connection issues"""

        def __init__(self, *, api_url: str):
            message = f"Failed to communicate with API at {api_url}"
            super().__init__(message)

    class DrsObjectNotFoundError(RuntimeError):
        """Raised when no DRS object was found with the specified DRS ID."""

        def __init__(self, *, drs_id: str):
            message = f"No DRS object with the following ID exists: {drs_id}"
            super().__init__(message)

    class EnvelopeNotFoundError(RuntimeError):
        """Raised when an envelope for a given download was not found"""

        def __init__(self, *, object_id: str):
            message = f"Envelope not found for object {object_id}"
            super().__init__(message)

    class RetryAccessLaterError(RuntimeError):
        """Raised when trying to access a DRS object that is not yet in the outbox.
        Instructs to retry later."""

        def __init__(self, *, retry_after: int):
            """Configure with the seconds after which a retry is should be performed."""

            self.retry_after = retry_after
            message = (
                "The requested DRS object is not yet accessible, please retry after"
                + f" {self.retry_after} seconds."
            )

            super().__init__(message)

    class UnexpectedAPIResponseError(RuntimeError):
        """Raise when API call returns unexpected return code"""

        def __init__(self, *, api_url: str, response_code: int):
            message = (
                f"Call to {api_url} returned unexpected response code {response_code}"
            )
            super().__init__(message)

    @abstractmethod
    async def access_drs_object(self, *, drs_id: str) -> models.DrsObjectResponseModel:
        """
        Serve the specified DRS object with access information.
        If it does not exists in the outbox, yet, a RetryAccessLaterError is raised that
        instructs to retry the call after a specified amount of time.
        """
        ...

    @abstractmethod
    async def register_new_file(self, *, file: models.DrsObject):
        """Register a file as a new DRS Object."""
        ...

    @abstractmethod
    async def serve_envelope(self, *, drs_id: str, public_key: str) -> str:
        """
        Retrieve envelope for the object with the given DRS ID

        :returns: base64 encoded envelope bytes
        """
        ...
