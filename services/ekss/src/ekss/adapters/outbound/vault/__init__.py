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
"""Module containing HashiCorp vault related functionality"""

from ekss.adapters.outbound.vault.client import VaultAdapter
from ekss.adapters.outbound.vault.exceptions import (
    SecretInsertionError,
    SecretRetrievalError,
)

__all__ = ["SecretInsertionError", "SecretRetrievalError", "VaultAdapter"]
