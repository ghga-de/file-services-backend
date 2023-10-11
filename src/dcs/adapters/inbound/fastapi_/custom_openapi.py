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

"""Utils to customize openAPI script"""
from typing import Any

from fastapi.openapi.utils import get_openapi

from dcs import __version__
from dcs.config import Config

config = Config()  # type: ignore


def get_openapi_schema(api) -> dict[str, Any]:
    """Generates a custom openapi schema for the service"""
    return get_openapi(
        title="Download Controller Service",
        version=__version__,
        description="A service managing access to file objects stored"
        + "on an S3-compatible Object Storage. "
        + "\n\nThis is an implementation of the DRS standard from the Global Alliance "
        + "for Genomics and Health, please find more information at: "
        + "https://github.com/ga4gh/data-repository-service-schemas",
        servers=[{"url": config.api_route}],
        tags=[{"name": "DownloadControllerService"}],
        routes=api.routes,
    )
