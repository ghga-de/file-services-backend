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
"""REST API configuration and function for CLI"""

import asyncio

from ghga_service_commons.api import run_server
from hexkit.log import configure_logging

from fins.config import Config
from fins.inject import (
    prepare_event_subscriber,
    prepare_outbox_subscriber,
    prepare_rest_app,
)


async def run_rest():
    """Run the HTTP REST API."""
    config = Config()
    configure_logging(config=config)

    async with prepare_rest_app(config=config) as app:
        await run_server(app=app, config=config)


async def consume_events(run_forever: bool = True):
    """Consume events for both the normal and outbox subscribers"""
    config = Config()
    configure_logging(config=config)

    async with (
        prepare_event_subscriber(config=config) as event_subscriber,
        prepare_outbox_subscriber(config=config) as outbox_subscriber,
    ):
        await asyncio.gather(
            outbox_subscriber.run(forever=run_forever),
            event_subscriber.run(forever=run_forever),
        )
