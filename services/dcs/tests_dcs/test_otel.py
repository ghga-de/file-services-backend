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

import base64
import inspect
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import httpx
import pytest
from hexkit.opentelemetry.testutils import (  # noqa: F401
    otel_fixture,
    otel_provider_fixture,
)
from hexkit.providers.s3.testutils import FileObject, tmp_file  # noqa: F401
from pytest_httpx import HTTPXMock, httpx_mock  # noqa: F401

from dcs import main
from dcs.adapters.outbound.http.secrets import SecretsClient, SecretsClientConfig
from dcs.constants import TRACER
from tests_dcs.fixtures.joint import PopulatedFixture
from tests_dcs.fixtures.mock_api.app import router
from tests_dcs.fixtures.utils import generate_work_order_token

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.httpx_mock(
        should_mock=lambda request: request.url.path.startswith("/ekss"),
    ),
]

ACCESSION = "GHGA001"


def _authorize(populated_fixture: PopulatedFixture) -> None:
    """Put a valid work order token on the fixture's REST client."""
    joint_fixture = populated_fixture.joint_fixture
    token = generate_work_order_token(
        file_id=populated_fixture.example_file.file_id,
        accession=ACCESSION,
        jwk=joint_fixture.jwk,
        valid_seconds=120,
    )
    joint_fixture.rest_client.headers = httpx.Headers(
        {"Authorization": f"Bearer {token}"}
    )


async def test_manual_span_recorded(otel):
    """TRACER is bound at import time, so this covers proxy tracer resolution too."""
    with TRACER.start_as_current_span("test-span"):
        pass

    otel.assert_has_span("test-span")


async def test_file_registration_records_spans(
    otel,  # first, so OpenTelemetry is configured before the fixtures below
    populated_fixture: PopulatedFixture,
):
    """`populated_fixture` publishes a registration event and runs the subscriber."""
    joint_fixture = populated_fixture.joint_fixture
    span_names = otel.get_span_names()

    otel.assert_has_span("EventSubTranslator._consume_files_to_register")
    otel.assert_has_span("EventPubTranslator.file_registered")

    db_name = joint_fixture.config.db_name
    assert [name for name in span_names if name.startswith(f"{db_name}.")], (
        f"No autoinstrumented MongoDB spans recorded. Captured: {span_names}"
    )

    # aiokafka producer spans are named "<topic> send"
    registered_topic = joint_fixture.config.file_registered_for_download_topic
    assert f"{registered_topic} send" in span_names, (
        f"No autoinstrumented Kafka span for topic {registered_topic!r}."
        f" Captured: {span_names}"
    )


async def test_drs_object_access_records_spans(
    otel,  # first, so OpenTelemetry is configured before the fixtures below
    populated_fixture: PopulatedFixture,
    tmp_file: FileObject,  # noqa: F811
):
    """First request publishes a staging request; the second reaches object storage."""
    joint_fixture = populated_fixture.joint_fixture
    drs_object = await populated_fixture.mongodb_dao.get_by_id(
        populated_fixture.example_file.file_id
    )
    _authorize(populated_fixture)

    otel.reset()
    response = await joint_fixture.rest_client.get(f"/objects/{ACCESSION}", timeout=5)
    assert response.status_code == 202

    otel.assert_has_span("routes.get_drs_object")
    staging_topic = joint_fixture.config.files_to_stage_topic
    assert f"{staging_topic} send" in otel.get_span_names(), (
        "No autoinstrumented Kafka span for the staging request."
        f" Captured: {otel.get_span_names()}"
    )

    # Stage the object so the retry actually builds a presigned URL
    file_object = tmp_file.model_copy(
        update={
            "bucket_id": joint_fixture.bucket_id,
            "object_id": str(drs_object.object_id),
        }
    )
    await joint_fixture.s3.populate_file_objects([file_object])

    otel.reset()
    response = await joint_fixture.rest_client.get(f"/objects/{ACCESSION}")
    assert response.status_code == 200

    span_names = otel.get_span_names()

    otel.assert_has_span("routes.get_drs_object")
    otel.assert_has_span("DataRepository._get_access_model")

    otel.assert_has_span("GET /objects/{object_id}")

    # Presigning is local-only; the span comes from the existence check behind it.
    assert "S3.HeadObject" in span_names, (
        f"No autoinstrumented S3 span for the object lookup. Captured: {span_names}"
    )

    db_name = joint_fixture.config.db_name
    assert [name for name in span_names if name.startswith(f"{db_name}.")], (
        f"No autoinstrumented MongoDB spans recorded. Captured: {span_names}"
    )


async def test_envelope_request_records_outbound_http_spans(
    otel,  # first, so OpenTelemetry is configured before the fixtures below
    populated_fixture: PopulatedFixture,
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """The only flow that leaves the service over HTTP."""
    joint_fixture = populated_fixture.joint_fixture
    httpx_mock.add_callback(
        callback=router.handle_request,
        url=re.compile(rf"^{joint_fixture.config.ekss_base_url}.*"),
    )
    _authorize(populated_fixture)

    otel.reset()
    response = await joint_fixture.rest_client.get(
        f"/objects/{ACCESSION}/envelopes", timeout=5
    )
    assert response.status_code == 200

    otel.assert_has_span("routes.get_envelope")
    otel.assert_has_span("api_calls.get_envelope_from_ekss")

    otel.assert_has_span("GET /objects/{object_id}/envelopes")

    # No httpx span here: the mock replaces the transport the instrumentation wraps.
    # `test_outbound_http_call_records_client_span` covers that against a real server.


async def test_outbound_http_call_records_client_span(otel):
    """Uses a real server, not `httpx_mock`: the mock replaces the wrapped transport."""
    public_key = base64.b64encode(b"0" * 32).decode()

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # the name is mandated by http.server
            body = json.dumps({"content": "envelope-bytes"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            """Silence the default stderr request logging."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        otel.reset()
        async with httpx.AsyncClient() as client:
            secrets_client = SecretsClient(
                config=SecretsClientConfig(ekss_base_url=base_url),
                httpx_client=client,
            )
            envelope = await secrets_client.get_envelope(
                secret_id="some-secret", receiver_public_key=public_key
            )
    finally:
        server.shutdown()
        thread.join(timeout=5)

    assert envelope == "envelope-bytes"

    otel.assert_has_span("api_calls.get_envelope_from_ekss")

    # httpx client spans are named "GET"
    client_span = otel.assert_has_span("GET")
    assert client_span.attributes
    assert client_span.attributes["http.status_code"] == 200


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
