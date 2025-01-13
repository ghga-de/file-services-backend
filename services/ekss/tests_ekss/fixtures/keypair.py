# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Provides a fixture around MongoDB, prefilling the DB with test data"""

import os
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkstemp

import pytest_asyncio
from crypt4gh.keys.c4gh import generate as generate_keypair
from fastapi.testclient import TestClient
from pydantic_settings import BaseSettings

from ekss.adapters.inbound.fastapi_.deps import config_injector
from ekss.adapters.inbound.fastapi_.main import setup_app
from ekss.config import Config
from tests_ekss.fixtures.config import SERVICE_CONFIG, get_config


@dataclass
class KeypairFixture:
    """Fixture containing a keypair"""

    public_key_path: Path
    private_key_path: Path


@pytest_asyncio.fixture
def generate_keypair_fixture() -> Generator[KeypairFixture, None]:
    """Creates a keypair using crypt4gh"""
    public_key_path, private_key_path = generate_new_keypair()
    yield KeypairFixture(
        public_key_path=public_key_path, private_key_path=private_key_path
    )

    public_key_path.unlink()
    private_key_path.unlink()


def generate_new_keypair(passphrase: str | None = None) -> tuple[Path, Path]:
    """Generate a new keypair in tmp"""
    # Crypt4GH always writes to file and umask inside of its code causes permission issues
    sk_file, sk_path = mkstemp(prefix="private", suffix=".key")
    pk_file, pk_path = mkstemp(prefix="public", suffix=".key")

    # Crypt4GH does not reset the umask it sets, so we need to deal with it
    # umask returns the current value before setting the specified mask
    original_umask = os.umask(0o022)
    if passphrase:
        generate_keypair(seckey=sk_file, pubkey=pk_file, passphrase=passphrase.encode())
    else:
        generate_keypair(seckey=sk_file, pubkey=pk_file)
    os.umask(original_umask)

    public_key_path = Path(pk_path)
    private_key_path = Path(sk_path)

    return public_key_path, private_key_path


def patch_config_and_app(config_to_patch: BaseSettings) -> tuple[Config, TestClient]:
    """Update key paths, inject new config, setup app and return config and test client"""
    public_key_path, private_key_path = generate_new_keypair(
        passphrase=SERVICE_CONFIG.private_key_passphrase
    )
    service_config = SERVICE_CONFIG.model_copy(
        update={
            "server_public_key_path": public_key_path,
            "server_private_key_path": private_key_path,
        }
    )

    config = get_config(sources=[config_to_patch, service_config])
    app = setup_app(config)
    app.dependency_overrides[config_injector] = lambda: config
    client = TestClient(app=app)
    return config, client
