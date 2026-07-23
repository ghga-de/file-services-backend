# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Tests that real service operations record spans across all instrumented backends."""

import inspect
from uuid import UUID

import pytest
from ghga_event_schemas.pydantic_ import FileDeletionRequested
from hexkit.correlation import new_correlation_id, set_correlation_id
from hexkit.opentelemetry.testutils import (  # noqa: F401
    otel_fixture,
    otel_provider_fixture,
)
from hexkit.utils import now_utc_ms_prec

from tests_ucs.fixtures import utils
from tests_ucs.fixtures.joint import JointFixture
from ucs import main
from ucs.constants import TRACER

pytestmark = pytest.mark.asyncio()


async def _create_box(joint_fixture: JointFixture) -> UUID:
    """Create a FileUploadBox through the REST API and return its ID."""
    token_header = utils.create_file_box_token_header(jwk=joint_fixture.rs_jwk)
    response = await joint_fixture.rest_client.post(
        "/boxes",
        json={"storage_alias": "test", "max_size": utils.TEST_MAX_BOX_SIZE},
        headers=token_header,
    )
    assert response.status_code == 201
    return UUID(response.json())


async def test_manual_span_recorded(otel):
    """TRACER is bound at import time, so this covers proxy tracer resolution too."""
    with TRACER.start_as_current_span("test-span"):
        pass

    otel.assert_has_span("test-span")


async def test_box_creation_records_spans_for_all_backends(
    otel,  # first, so OpenTelemetry is configured before the fixtures below
    joint_fixture: JointFixture,
):
    """One request should produce REST, MongoDB and Kafka spans, plus the manual one."""
    otel.reset()
    box_id = await _create_box(joint_fixture)
    span_names = otel.get_span_names()

    otel.assert_has_span("routes.create_box")

    server_span = otel.assert_has_span("POST /boxes")
    assert server_span.attributes
    assert server_span.attributes["http.status_code"] == 201

    # pymongo spans are named "<collection>.<command>"
    db_name = joint_fixture.config.db_name
    assert [name for name in span_names if name.startswith(f"{db_name}.")], (
        f"No autoinstrumented MongoDB spans recorded. Captured: {span_names}"
    )

    # aiokafka producer spans are named "<topic> send"
    box_topic = joint_fixture.config.file_upload_box_topic
    assert f"{box_topic} send" in span_names, (
        f"No autoinstrumented Kafka span for topic {box_topic!r}."
        f" Captured: {span_names}"
    )

    assert box_id  # the flow really did complete

    # Without the parent link the manual span would be detached from the trace.
    route_span = otel.assert_has_span("routes.create_box")
    assert route_span.parent is not None
    assert route_span.parent.span_id == server_span.context.span_id
    assert route_span.context.trace_id == server_span.context.trace_id


async def test_file_upload_creation_records_s3_spans(
    otel,  # first, so OpenTelemetry is configured before the fixtures below
    joint_fixture: JointFixture,
):
    """The only flow that reaches object storage, so it covers botocore."""
    box_id = await _create_box(joint_fixture)

    otel.reset()
    token_header = utils.create_file_token_header(
        jwk=joint_fixture.wps_jwk, box_id=box_id, alias="test_file"
    )
    response = await joint_fixture.rest_client.post(
        f"/boxes/{box_id}/uploads",
        json={
            "alias": "test_file",
            "decrypted_size": utils.DECRYPTED_SIZE,
            "encrypted_size": utils.ENCRYPTED_SIZE,
            "part_size": utils.PART_SIZE,
        },
        headers=token_header,
    )
    assert response.status_code == 201

    span_names = otel.get_span_names()

    otel.assert_has_span("routes.create_file_upload")

    otel.assert_has_span("POST /boxes/{box_id}/uploads")

    assert "S3.CreateMultipartUpload" in span_names, (
        f"No autoinstrumented S3 span for the multipart upload. Captured: {span_names}"
    )


async def test_consumed_event_records_spans(
    otel,  # first, so OpenTelemetry is configured before the fixtures below
    joint_fixture: JointFixture,
):
    """Event consumption is a separate entry point from the REST API."""
    box_id = await _create_box(joint_fixture)

    event = FileDeletionRequested(file_id=box_id, created=now_utc_ms_prec())
    await joint_fixture.kafka.publish_event(
        payload=event.model_dump(mode="json"),
        type_=joint_fixture.config.file_deletion_request_type,
        topic=joint_fixture.config.file_deletion_request_topic,
        key=str(box_id),
    )

    otel.reset()
    async with set_correlation_id(new_correlation_id()):
        await joint_fixture.event_subscriber.run(forever=False)

    span_names = otel.get_span_names()

    otel.assert_has_span("EventSubTranslator._consume_file_deletion_requested")

    # Autoinstrumented MongoDB spans from the resulting lookups
    db_name = joint_fixture.config.db_name
    assert [name for name in span_names if name.startswith(f"{db_name}.")], (
        f"No autoinstrumented MongoDB spans recorded. Captured: {span_names}"
    )


async def test_long_running_entrypoints_configure_otel():
    """An entrypoint that skips it emits no traces at all.

    `migrate_db` is excluded by convention: a one-off command, not a service.
    """
    excluded = {"migrate_db"}
    entrypoints = {
        name: obj
        for name, obj in vars(main).items()
        if inspect.iscoroutinefunction(obj) and obj.__module__ == main.__name__
    }
    assert entrypoints, "No entrypoints found - has main.py been restructured?"

    missing = sorted(
        name
        for name, func in entrypoints.items()
        if name not in excluded
        and "configure_opentelemetry" not in inspect.getsource(func)
    )
    assert not missing, f"Entrypoints not configuring OpenTelemetry: {missing}"
