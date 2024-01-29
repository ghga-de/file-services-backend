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

"""Interfaces for event publishing adapters and the exception they may throw."""

from datetime import datetime
from typing import Protocol

from ucs.core import models


class EventPublisherPort(Protocol):
    """An interface for an adapter that publishes events happening to this service."""

    async def publish_upload_received(  # noqa: PLR0913
        self,
        *,
        file_metadata: models.FileMetadata,
        upload_date: datetime,
        submitter_public_key: str,
        object_id: str,
        bucket_id: str,
        storage_alias: str,
    ) -> None:
        """Publish event informing that a new upload was received."""
        ...
