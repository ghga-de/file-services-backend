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
#

"""
Adds wrapper classes to translate httpyexpect errors and check against
provided exception specs for all API endpoints
"""

import httpx
from ghga_service_commons.httpyexpect.client import ExceptionMapping, ResponseTranslator


class ResponseExceptionTranslator:
    """Base class providing behaviour and injection point for spec"""

    def __init__(self, *, spec: dict[int, object]) -> None:
        self._exception_map = ExceptionMapping(spec)

    def handle(self, response: httpx.Response):
        """Translate and raise error, if defined by spec"""
        translator = ResponseTranslator(response, exception_map=self._exception_map)
        translator.raise_for_error()
