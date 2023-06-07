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
"""Adapter for publishing events to other services."""

from hexkit.protocols.eventpub import EventPublisherProtocol
from pydantic import BaseSettings

from fis.ports.outbound.event_pub import EventPublisherPort


class EventPubTranslatorConfig(BaseSettings):
    """Config for publishing events to other file backend services."""


class EventPubTranslator(EventPublisherPort):
    """A translator according to  the triple hexagonal architecture implementing
    the EventPublisherPort."""

    def __init__(
        self, *, config: EventPubTranslatorConfig, provider: EventPublisherProtocol
    ):
        """Initialize with configs and a provider of the EventPublisherProtocol."""

        self._config = config
        self._provider = provider
