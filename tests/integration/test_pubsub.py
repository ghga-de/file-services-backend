# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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


"""Test the messaging API (pubsub)"""

from typing import Any, Callable, Dict

from ghga_message_schemas import schemas
from ghga_service_chassis_lib.utils import exec_with_timeout

from dcs.pubsub import subscribe_file_registered

from ..fixtures import (  # noqa: F401
    DEFAULT_CONFIG,
    FILES,
    amqp_fixture,
    get_config,
    psql_fixture,
    s3_fixture,
)


def sub_and_pub_test_generic(
    upstream_topic_name: str,
    downstream_topic_name: str,
    upstream_message: Dict[str, Any],
    upstream_msg_schema: dict,
    downstream_msg_schema: dict,
    subscribe_func: Callable,
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
    amqp_fixture,  # noqa: F811
):
    """Generic function for testing functions that first subscribe and then publish."""

    config = get_config(
        sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
    )

    # initialize upstream and downstream test services that will publish or receive
    # messages to or from this service:
    upstream_publisher = amqp_fixture.get_test_publisher(
        topic_name=upstream_topic_name,
        message_schema=upstream_msg_schema,
    )

    downstream_subscriber = amqp_fixture.get_test_subscriber(
        topic_name=downstream_topic_name,
        message_schema=downstream_msg_schema,
    )

    # publish a stage request:
    upstream_publisher.publish(upstream_message)

    # process the stage request:
    exec_with_timeout(
        func=lambda: subscribe_func(config=config, run_forever=False),
        timeout_after=2,
    )

    # expect stage confirmation message:
    downstream_message = downstream_subscriber.subscribe(timeout_after=2)
    assert downstream_message["file_id"] == upstream_message["file_id"]


def test_subscribe_stage_requests(psql_fixture, s3_fixture, amqp_fixture):  # noqa: F811
    """Test `subscribe_stage_requests` function"""

    sub_and_pub_test_generic(
        upstream_topic_name=DEFAULT_CONFIG.topic_name_file_registered,
        downstream_topic_name=DEFAULT_CONFIG.topic_name_drs_object_registered,
        upstream_message=FILES["not_in_registry_not_in_storage"].message,
        upstream_msg_schema=schemas.SCHEMAS["file_internally_registered"],
        downstream_msg_schema=schemas.SCHEMAS["drs_object_registered"],
        subscribe_func=subscribe_file_registered,
        psql_fixture=psql_fixture,
        s3_fixture=s3_fixture,
        amqp_fixture=amqp_fixture,
    )
