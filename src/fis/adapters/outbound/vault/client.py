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
"""Provides client side functionality for interaction with HashiCorp Vault"""

from typing import Union
from uuid import uuid4

import hvac
import hvac.exceptions
from pydantic import BaseSettings, Field, SecretStr

from fis.ports.outbound.vault.client import VaultAdapterPort


class VaultConfig(BaseSettings):
    """Configuration for HashiCorp Vault connection"""

    vault_url: str = Field(
        ...,
        example="http://127.0.0.1.8200",
        description="URL of the vault instance to connect to",
    )
    vault_role_id: SecretStr = Field(
        ...,
        example="example_role",
        description="Vault role ID to access a specific prefix",
    )
    vault_secret_id: SecretStr = Field(
        ...,
        example="example_secret",
        description="Vault secret ID to access a specific prefix",
    )
    vault_verify: Union[bool, str] = Field(
        True,
        example="/etc/ssl/certs/my_bundle.pem",
        description="SSL certificates (CA bundle) used to"
        " verify the identity of the vault, or True to"
        " use the default CAs, or False for no verification.",
    )


class VaultAdapter(VaultAdapterPort):
    """Adapter wrapping hvac.Client"""

    def __init__(self, config: VaultConfig):
        """Initialized approle based client and login"""
        self._client = hvac.Client(url=config.vault_url, verify=config.vault_verify)

        self._role_id = config.vault_role_id.get_secret_value()
        self._secret_id = config.vault_secret_id.get_secret_value()

    def _check_auth(self):
        """Check if authentication timed out and re-authenticate if needed"""
        if not self._client.is_authenticated():
            self._login()

    def _login(self):
        """Log in using role ID and secret ID"""
        self._client.auth.approle.login(
            role_id=self._role_id, secret_id=self._secret_id
        )

    def store_secret(self, *, secret: str, prefix: str = "ekss") -> str:
        """
        Store a secret under a subpath of the given prefix.
        Generates a UUID4 as key, uses it for the subpath and returns it.
        """
        key = str(uuid4())

        self._check_auth()

        try:
            # set cas to 0 as we only want a static secret
            self._client.secrets.kv.v2.create_or_update_secret(
                path=f"{prefix}/{key}", secret={key: secret}, cas=0
            )
        except hvac.exceptions.InvalidRequest as exc:
            raise self.SecretInsertionError() from exc
        return key
