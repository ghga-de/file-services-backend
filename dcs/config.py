# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

from typing import Any

from ghga_service_commons.api import ApiConfigBase
from ghga_service_commons.auth.ghga import AuthConfig
from hexkit.config import config_from_yaml
from hexkit.providers.akafka import KafkaConfig
from hexkit.providers.mongodb import MongoDbConfig
from hexkit.providers.s3 import S3Config
from pydantic import Field

from dcs.adapters.inbound.event_sub import EventSubTranslatorConfig
from dcs.adapters.outbound.event_pub import EventPubTranslatorConfig
from dcs.core.data_repository import DataRepositoryConfig


class WorkOrderTokenConfig(AuthConfig):
    """Overwrite checked claims"""

    auth_check_claims: dict[str, Any] = Field(
        dict.fromkeys(
            "type file_id user_id user_public_crypt4gh_key full_user_name email iat exp".split()
        ),
        description="A dict of all GHGA internal claims that shall be verified.",
    )


# pylint: disable=too-many-ancestors
@config_from_yaml(prefix="dcs")
class Config(
    ApiConfigBase,
    AuthConfig,
    S3Config,
    DataRepositoryConfig,
    MongoDbConfig,
    KafkaConfig,
    EventPubTranslatorConfig,
    EventSubTranslatorConfig,
):
    """Config parameters and their defaults."""

    service_name: str = "dcs"
    api_route: str = "/ga4gh/drs/v1"
