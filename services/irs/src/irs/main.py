# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
#
"""Top-level object construction and dependency injection"""

from hexkit.log import configure_logging
from hexkit.opentelemetry import configure_opentelemetry

from irs.config import Config
from irs.inject import (
    get_persistent_publisher,
    prepare_event_subscriber,
    prepare_storage_inspector,
)


async def consume_events(run_forever: bool = True):
    """Run the event consumer"""
    config = Config()
    configure_logging(config=config)
    configure_opentelemetry(service_name=config.service_name, config=config)

    async with prepare_event_subscriber(config=config) as event_subscriber:
        await event_subscriber.run(forever=run_forever)


async def check_staging_buckets():
    """Run a job to inspect all configured storage buckets for stale objects.

    For now this only logs objects that should no longer remain in their respective bucket,
    but have not been removed by the mechanisms in place.
    """
    config = Config()
    configure_logging(config=config)
    configure_opentelemetry(service_name=config.service_name, config=config)

    async with prepare_storage_inspector(config=config) as staging_inspector:
        await staging_inspector.check_buckets()


async def publish_events(*, all: bool = False):
    """Publish pending events. Set `--all` to (re)publish all events regardless of status."""
    config = Config()
    configure_logging(config=config)
    configure_opentelemetry(service_name=config.service_name, config=config)

    async with get_persistent_publisher(config=config) as persistent_publisher:
        if all:
            await persistent_publisher.republish()
        else:
            await persistent_publisher.publish_pending()
