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

"""
Publish asynchronous topics
"""

from pathlib import Path

from ghga_message_schemas import schemas
from ghga_service_chassis_lib.pubsub import AmqpTopic

from .. import models
from ..config import CONFIG, Config

HERE = Path(__file__).parent.resolve()


def publish_stage_request(drs_object: models.DrsObjectBase, config: Config = CONFIG):
    """
    Publishes a message to the non_staged_file_requested topic
    """

    topic_name = config.topic_name_stage_request

    message = {
        "file_id": drs_object.file_id,
        "md5_checksum": drs_object.md5_checksum,
        "size": drs_object.size,
        "creation_date": drs_object.creation_date.isoformat(),
        "update_date": drs_object.update_date.isoformat(),
        "format": drs_object.format,
    }

    # create a topic object:
    topic = AmqpTopic(
        config=config,
        topic_name=topic_name,
        json_schema=schemas.SCHEMAS["non_staged_file_requested"],
    )

    topic.publish(message)


def publish_drs_object_registered(
    drs_object: models.DrsObjectBase, config: Config = CONFIG
):
    """
    Publishes a message to the drs_object_registered topic
    """

    topic_name = config.topic_name_drs_object_registered

    message = {
        "file_id": drs_object.file_id,
        "drs_uri": f"{config.drs_self_url}/{drs_object.file_id}",
        "md5_checksum": drs_object.md5_checksum,
        "size": drs_object.size,
        "creation_date": drs_object.creation_date.isoformat(),
        "update_date": drs_object.update_date.isoformat(),
        "format": drs_object.format,
    }

    # create a topic object:
    topic = AmqpTopic(
        config=config,
        topic_name=topic_name,
        json_schema=schemas.SCHEMAS["drs_object_registered"],
    )

    topic.publish(message)
