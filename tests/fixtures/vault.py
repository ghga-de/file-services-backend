# Copyright 2021 - 2023 Universität Tübingen, DKFZ and EMBL
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
"""HashiCorp vault fixture for texting"""

import time
from dataclasses import dataclass
from typing import Generator

import hvac
import pytest
from testcontainers.general import DockerContainer

from ekss.adapters.inbound.fastapi_.deps import VaultConfig
from ekss.adapters.outbound.vault.client import VaultAdapter

VAULT_ADDR = "http://0.0.0.0:8200"
VAULT_NAMESPACE = "vault"
VAULT_TOKEN = "dev-token"
VAULT_PORT = 8200


@dataclass
class VaultFixture:
    """Contains initialized vault client"""

    adapter: VaultAdapter
    config: VaultConfig


@pytest.fixture
def vault_fixture() -> Generator[VaultFixture, None, None]:
    """Generate preconfigured test container"""
    vault_container = (
        DockerContainer(image="hashicorp/vault:1.11.4")
        .with_exposed_ports(VAULT_PORT)
        .with_env("VAULT_ADDR", VAULT_ADDR)
        .with_env("VAULT_DEV_ROOT_TOKEN_ID", VAULT_TOKEN)
    )
    with vault_container:
        host = vault_container.get_container_host_ip()
        port = vault_container.get_exposed_port(VAULT_PORT)
        url = f"http://{host}:{port}"
        client = hvac.Client(url=url, token=VAULT_TOKEN, namespace=VAULT_NAMESPACE)
        vault_client = VaultAdapter(client=client)
        # necessary for now
        time.sleep(2)
        yield VaultFixture(
            adapter=vault_client,
            config=VaultConfig(
                vault_host=f"http://{host}", vault_port=port, vault_token=VAULT_TOKEN
            ),
        )
