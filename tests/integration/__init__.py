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
Package containing integration tests.
The __init__ module contains a skeleton of the test framework.
"""


import unittest

from webtest import TestApp

from dcs.api.main import get_app
from dcs.config import CONFIG, Config


class BaseIntegrationTest(unittest.TestCase):
    """Base TestCase to inherit from"""

    def setUp(self):
        """Setup Test Server"""
        self.config: Config = CONFIG
        app = get_app(config=self.config)
        self.testapp = TestApp(app)

    def tearDown(self):
        """Teardown Test Server"""
        del self.testapp
