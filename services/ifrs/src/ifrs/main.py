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

"""In this module object construction and dependency injection is carried out."""

from hexkit.log import configure_logging

from ifrs.config import Config
from ifrs.inject import prepare_outbox_subscriber
from ifrs.migrations import run_db_migrations


async def consume_events(run_forever: bool = True):
    """Run an event consumer listening to the specified topics."""
    config = Config()
    configure_logging(config=config)
    await run_db_migrations(config=config)

    async with prepare_outbox_subscriber(config=config) as outbox_subscriber:
        await outbox_subscriber.run(forever=run_forever)
