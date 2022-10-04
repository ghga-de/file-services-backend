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


from ghga_message_schemas import schemas
from ghga_service_chassis_lib.utils import exec_with_timeout

from dcs.pubsub import publish_stage_request, subscribe_file_staged

from ..fixtures import (  # noqa: F401
    FILES,
    amqp_fixture,
    get_config,
    psql_fixture,
    s3_fixture,
)


def test_publish_stage_request(amqp_fixture):  # noqa: F811
    """Test publishing `non_staged_file_requested` topic"""

    config = get_config(sources=[amqp_fixture.config])

    drs_object = FILES["in_registry_not_in_storage"].file_info

    # initialize downstream test service that will receive
    # messages from this service:

    downstream_subscriber = amqp_fixture.get_test_subscriber(
        topic_name=config.topic_name_stage_request,
        message_schema=schemas.SCHEMAS["non_staged_file_requested"],
    )

    # Call publish function

    publish_stage_request(drs_object=drs_object, config=config)

    # expect stage confirmation message:
    downstream_message = downstream_subscriber.subscribe(timeout_after=2)
    assert downstream_message["file_id"] == FILES["in_registry_not_in_storage"].file_id


def test_subscribe_file_staged(psql_fixture, s3_fixture, amqp_fixture):  # noqa: F811
    """Test subscribing to `file_staged_for_download` topic"""
    config = get_config(
        sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
    )

    # initialize upstream and downstream test services that will publish or receive
    upstream_publisher = amqp_fixture.get_test_publisher(
        topic_name=config.topic_name_file_staged,
        message_schema=schemas.SCHEMAS["file_staged_for_download"],
    )

    # publish a stage request:
    upstream_publisher.publish(FILES["in_registry_in_storage"].message)

    exec_with_timeout(
        func=lambda: subscribe_file_staged(config=config, run_forever=False),
        timeout_after=2,
    )
