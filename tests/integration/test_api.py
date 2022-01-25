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

"""Test the api module"""


from . import BaseIntegrationTest


class TestBase(BaseIntegrationTest):
    """Test whether Basic API functions are reachable."""

    def test_swagger_api_loaded(self):
        """Swagger's API Explorer should be served on the api_route"""
        response = self.testapp.get(self.config.api_route, status=200)
        assert (
            "<title>Swagger UI</title>" in response.text
        ), "Swagger UI could not be loaded"

    def test_health(self):
        """The health check should be up, running and served on /health"""
        response = self.testapp.get("/health", status=200)
        assert response.json == {"status": "OK"}
