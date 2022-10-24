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

"""Config Parameter Modeling and Parsing"""

from typing import Literal

from ghga_service_chassis_lib.api import ApiConfigBase
from ghga_service_chassis_lib.config import config_from_yaml
from ghga_service_chassis_lib.postgresql import PostgresqlConfigBase
from ghga_service_chassis_lib.s3 import S3ConfigBase

LogLevel = Literal["critical", "error", "warning", "info", "debug", "trace"]


@config_from_yaml(prefix="dcs")
class Config(ApiConfigBase, PostgresqlConfigBase, S3ConfigBase):
    """Config parameters and their defaults."""

    service_name: str = "dcs"
    api_route: str = "/ga4gh/drs/v1"
    drs_self_url: str = "drs://localhost:8080/"
    topic_name_stage_request: str = "non_staged_file_requested"
    topic_name_file_staged: str = "file_staged_for_download"
    topic_name_file_registered: str = "file-internally-registered"
    topic_name_drs_object_registered: str = "drs-object-registered"

    s3_outbox_bucket_id: str


CONFIG = Config()
