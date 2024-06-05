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

"""In this module object construction and dependency injection is carried out."""

from ghga_service_commons.api import run_server
from hexkit.log import configure_logging

from ucs.config import Config
from ucs.inject import (
    prepare_event_subscriber,
    prepare_rest_app,
    prepare_storage_inspector,
)


async def run_rest_app():
    """Run the HTTP REST API."""
    config = Config()
    configure_logging(config=config)

    async with prepare_rest_app(config=config) as app:
        await run_server(app=app, config=config)


async def consume_events(run_forever: bool = True):
    """Run an event consumer listening to the specified topic."""
    config = Config()
    configure_logging(config=config)

    async with prepare_event_subscriber(config=config) as event_subscriber:
        await event_subscriber.run(forever=run_forever)


async def check_inbox_buckets():
    """Run a job to inspect all configured storage buckets for stale objects.

    For now this only logs objects that should no longer remain in their respective bucket,
    but have not been removed by the mechanisms in place.
    """
    config = Config()
    configure_logging(config=config)

    async with prepare_storage_inspector(config=config) as inbox_inspector:
        await inbox_inspector.check_buckets()