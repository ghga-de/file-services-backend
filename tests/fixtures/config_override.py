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
"""Provides context manager for config overrides"""

from ekss.config import CONFIG

from .config import CONFIG as TEST_CONFIG


class ConfigOverride:
    """Override server private key in the context"""

    def __init__(self):
        self.original_private_key = ""
        self.original_pubkey = ""

    def __enter__(self):
        self.original_private_key = CONFIG.server_private_key
        self.original_pubkey = CONFIG.server_publick_key

        CONFIG.server_private_key = TEST_CONFIG.server_private_key
        CONFIG.server_publick_key = TEST_CONFIG.server_private_key

    def __exit__(self, exc_type, exc_value, exc_traceback):
        CONFIG.server_private_key = self.original_private_key
        CONFIG.server_publick_key = self.original_pubkey
