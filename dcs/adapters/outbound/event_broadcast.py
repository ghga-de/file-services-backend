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

"""Interface for broadcasting events to other services."""

from dcs.core import models
from dcs.ports.outbound.event_broadcast import DrsEventBroadcasterPort


class DrsEventBroadcaster(DrsEventBroadcasterPort):
    """An implementation for the DrsEventBroadcasterPort."""

    async def download_served(self, *, drs_object: models.DrsObjectWithUri) -> None:
        """Communicate the event of an download being served. This can be relevant for
        auditing purposes."""

    async def unstaged_download_requested(
        self, *, drs_object: models.DrsObjectWithUri
    ) -> None:
        """Communicates the event that a download was requested for a DRS object, that
        is not yet available in the outbox."""

    async def new_drs_object_registered(
        self, *, drs_object: models.DrsObjectWithUri
    ) -> None:
        """Communicates the event that a file has been registered."""
