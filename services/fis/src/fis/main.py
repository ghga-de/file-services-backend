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

from ghga_service_commons.api import run_server
from hexkit.log import configure_logging

from fis.config import Config
from fis.inject import get_file_validation_success_dao, prepare_rest_app


async def run_rest():
    """Run the HTTP REST API."""
    config = Config()
    configure_logging(config=config)

    async with prepare_rest_app(config=config) as app:
        await run_server(app=app, config=config)


async def publish_events(*, all: bool = False):
    """Publish pending events.

    If `all` is True, it will additionally republish all outbox events that have already
    been published.
    """
    config = Config()
    configure_logging(config=config)

    async with get_file_validation_success_dao(config=config) as dao:
        if all:
            await dao.republish()
        else:
            await dao.publish_pending()
