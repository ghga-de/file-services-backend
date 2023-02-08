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

import base64
from uuid import uuid4

import hvac
import hvac.exceptions

from ekss.adapters.outbound.vault import exceptions
from ekss.config import VaultConfig


class VaultAdapter:
    """Adapter wrapping hvac.Client"""

    def __init__(self, config: VaultConfig):
        """Initialized approle based client and login"""
        protocol = "http" if config.debug_vault else "https"
        url = f"{protocol}://{config.vault_host}:{config.vault_port}"
        self._client = hvac.Client(url=url)

        self._client.secrets.kv.default_kv_version = 2
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

    def store_secret(self, *, secret: bytes, prefix: str = "ekss") -> str:
        """
        Store a secret under a subpath of the given prefix.
        Generates a UUID4 as key, uses it for the subpath and returns it.
        """
        value = base64.b64encode(secret).decode("utf-8")
        key = str(uuid4())

        self._check_auth()

        try:
            # set cas to 0 as we only want a static secret
            self._client.secrets.kv.create_or_update_secret(
                path=f"{prefix}/{key}", secret={key: value}, cas=0
            )
        except hvac.exceptions.InvalidRequest as exc:
            raise exceptions.SecretInsertionError() from exc
        return key

    def get_secret(self, *, key: str, prefix: str = "ekss") -> bytes:
        """
        Retrieve a secret at the subpath of the given prefix denoted by key.
        Key should be a UUID4 returned by store_secret on insertion
        """

        self._check_auth()

        try:
            response = self._client.secrets.kv.read_secret_version(
                path=f"{prefix}/{key}"
            )
        except hvac.exceptions.InvalidPath as exc:
            raise exceptions.SecretRetrievalError() from exc

        secret = response["data"]["data"][key]
        return base64.b64decode(secret)
