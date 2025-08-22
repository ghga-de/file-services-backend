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

"""Top-level service functions"""

from ghga_service_commons.api import run_server
from hexkit.log import configure_logging
from hexkit.opentelemetry import configure_opentelemetry

from ucs.config import Config
from ucs.inject import prepare_outbox_publisher, prepare_rest_app


async def run_rest_app():
    """Run the HTTP REST API."""
    config = Config()
    configure_logging(config=config)
    configure_opentelemetry(service_name=config.service_name, config=config)

    async with prepare_rest_app(config=config) as app:
        await run_server(app=app, config=config)


async def publish_events(*, all: bool = False):
    """Publish pending events. Set `--all` to (re)publish all events regardless of status."""
    config = Config()
    configure_logging(config=config)
    configure_opentelemetry(service_name=config.service_name, config=config)

    async with prepare_outbox_publisher(config=config) as persistent_publisher:
        file_upload_box_dao = await persistent_publisher.get_file_upload_box_dao()
        file_upload_dao = await persistent_publisher.get_file_upload_dao()
        if all:
            await file_upload_box_dao.republish()
            await file_upload_dao.republish()
        else:
            await file_upload_box_dao.publish_pending()
            await file_upload_dao.publish_pending()
