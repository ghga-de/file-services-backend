# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Contains interfaces for public file information storage, retrieval and deletion."""

from abc import ABC, abstractmethod

import ghga_event_schemas.pydantic_ as event_schemas

from fins.core.models import FileInformation


class InformationServicePort(ABC):
    """Abstract baseclass for a service that handles storage and deletion of relevant
    metadata for files registered with the Internal File Registry service.
    """

    class MismatchingInformationAlreadyRegistered(RuntimeError):
        """Raised when information for a given file ID is not registered."""

        def __init__(self, *, file_id: str):
            message = f"Mismatching information for the file with ID {
                file_id} has already been registered."
            super().__init__(message)

    class InformationNotFoundError(RuntimeError):
        """Raised when information for a given file ID is not registered."""

        def __init__(self, *, file_id: str):
            message = f"Information for the file with ID {
                file_id} is not registered."
            super().__init__(message)

    @abstractmethod
    async def deletion_requested(self, file_id: str):
        """Handle deletion requestes for information associated with the give file ID."""

    @abstractmethod
    async def register_information(
        self, file_registered: event_schemas.FileInternallyRegistered
    ):
        """Store information for a file newly registered with the Internal File Registry."""

    @abstractmethod
    async def serve_information(self, file_id: str) -> FileInformation:
        """Retrieve stored public information for the five file ID to be served by API."""
