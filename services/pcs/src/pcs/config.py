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

"""Config Parameter Modeling and Parsing"""

from ghga_service_commons.api import ApiConfigBase
from hexkit.config import config_from_yaml
from hexkit.log import LoggingConfig
from hexkit.providers.akafka import KafkaConfig
from hexkit.providers.mongokafka import MongoKafkaConfig

from pcs.adapters.inbound.fastapi_.config import TokenHashConfig
from pcs.adapters.outbound.event_pub import EventPubTranslatorConfig

SERVICE_NAME = "pcs"


@config_from_yaml(prefix=SERVICE_NAME)
class Config(
    ApiConfigBase,
    MongoKafkaConfig,
    KafkaConfig,
    TokenHashConfig,
    LoggingConfig,
    EventPubTranslatorConfig,
):
    """Config parameters and their defaults."""

    service_name: str = SERVICE_NAME
