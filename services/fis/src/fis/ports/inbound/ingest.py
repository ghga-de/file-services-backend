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
"""Ports for S3 upload metadata ingest"""

from abc import ABC, abstractmethod

from pydantic import SecretStr

from fis.core import models


class DecryptionError(RuntimeError):
    """Raised when decryption with the provided private key failed"""

    def __init__(self):
        message = "Could not decrypt received payload with the given key."
        super().__init__(message)


class VaultCommunicationError(RuntimeError):
    """Raised when interaction with the vault resulted in an error"""

    def __init__(self, *, message) -> None:
        super().__init__(message)


class WrongDecryptedFormatError(RuntimeError):
    """Raised when the decrypted payload"""

    def __init__(self, *, cause: str):
        message = f"Decrypted payload does not conform to expected format: {cause}."
        super().__init__(message)


class LegacyUploadMetadataProcessorPort(ABC):
    """Port for legacy S3 upload metadata processor"""

    @abstractmethod
    async def decrypt_payload(
        self, *, encrypted: models.EncryptedPayload
    ) -> models.LegacyUploadMetadata:
        """Decrypt upload metadata using private key"""
        ...

    @abstractmethod
    async def has_already_been_processed(self, *, file_id: str) -> bool:
        """Check if file metadata has already been seen and successfully processed."""
        ...

    @abstractmethod
    async def populate_by_event(
        self, *, upload_metadata: models.LegacyUploadMetadata, secret_id: str
    ):
        """Send FileUploadValidationSuccess event to be processed by downstream services"""
        ...

    @abstractmethod
    async def store_secret(self, *, file_secret: SecretStr) -> str:
        """Communicate with HashiCorp Vault to store file secret and get secret ID"""
        ...


class UploadMetadataProcessorPort(ABC):
    """Port for S3 upload metadata processor"""

    @abstractmethod
    async def decrypt_secret(self, *, encrypted: models.EncryptedPayload) -> SecretStr:
        """Decrypt file secret payload"""
        ...

    @abstractmethod
    async def has_already_been_processed(self, *, file_id: str) -> bool:
        """Check if file metadata has already been seen and successfully processed."""
        ...

    @abstractmethod
    async def populate_by_event(
        self, *, upload_metadata: models.UploadMetadata, secret_id: str
    ):
        """Send FileUploadValidationSuccess event to be processed by downstream services"""
        ...

    @abstractmethod
    async def store_secret(self, *, file_secret: SecretStr) -> str:
        """Communicate with HashiCorp Vault to store file secret and get secret ID"""
        ...
