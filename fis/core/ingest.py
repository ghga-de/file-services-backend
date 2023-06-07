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
"""Functionality relating to S3 upload metadata processing"""

import json

from ghga_event_schemas.pydantic_ import FileUploadValidationSuccess
from ghga_service_commons.utils.crypt import decrypt
from nacl.exceptions import CryptoError
from pydantic import BaseSettings, ValidationError

from fis.core import models
from fis.ports.inbound.ingest import UploadMetadataProcessorPort


class ServiceConfig(BaseSettings):
    """Specific configs for authentication and encryption"""

    token_hashes: list[str]
    private_key: str


class UploadMetadataProcessor(UploadMetadataProcessorPort):
    """Handler for S3 upload metadata processing"""

    def __init__(self, *, config: ServiceConfig):
        self._config = config

    async def decrypt_payload(
        self, *, encrypted: models.FileUploadMetadataEncrypted
    ) -> models.FileUploadMetadata:
        """Decrypt upload metadata using private key"""
        try:
            decrypted = decrypt(data=encrypted.payload, key=self._config.private_key)
        except (ValueError, CryptoError) as error:
            raise self.DecryptionError() from error

        upload_metadata = json.loads(decrypted)

        try:
            return models.FileUploadMetadata(**upload_metadata)
        except ValidationError as error:
            raise self.WrongDecryptedFormatError(cause=str(error)) from error

    async def populate_by_event(self, *, event: FileUploadValidationSuccess):
        """Send FileUploadValidationSuccess event to be processed by downstream services"""

    async def store_secret(self, *, file_secret: str):
        """Communicate with HashiCorp Vault to store file secret and get secret ID"""
