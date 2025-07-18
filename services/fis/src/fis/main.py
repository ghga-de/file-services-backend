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
"""REST API configuration and function for CLI"""

from ghga_service_commons.api import run_server
from hexkit.log import configure_logging
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio

from fis.config import Config
from fis.inject import get_persistent_publisher, prepare_rest_app
from fis.migrations import run_db_migrations

DB_VERSION = 2


async def run_rest():
    """Run the HTTP REST API."""
    config = Config()
    configure_logging(config=config)
    configure_opentelemetry(config=config)

    await run_db_migrations(config=config, target_version=DB_VERSION)

    async with prepare_rest_app(config=config) as app:
        await run_server(app=app, config=config)


async def publish_events(*, all: bool = False):
    """Publish pending events. Set `--all` to (re)publish all events regardless of status."""
    config = Config()
    configure_logging(config=config)
    configure_opentelemetry(config=config)

    await run_db_migrations(config=config, target_version=DB_VERSION)

    async with get_persistent_publisher(config=config) as persistent_publisher:
        if all:
            await persistent_publisher.republish()
        else:
            await persistent_publisher.publish_pending()


def configure_opentelemetry(config: Config):
    """Configure OpenTelemetry for the service."""
    resource = Resource(attributes={SERVICE_NAME: config.service_name})
    sampler = ParentBasedTraceIdRatio(rate=config.otel_trace_sampling_rate)

    # Initialize service specific TracerProvider
    trace_provider = TracerProvider(resource=resource, sampler=sampler)
    processor = BatchSpanProcessor(OTLPSpanExporter())
    trace_provider.add_span_processor(processor)
    trace.set_tracer_provider(trace_provider)
