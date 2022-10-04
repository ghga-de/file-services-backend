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
Provides DataTransferObjects to handle GHGA secrets in Mongo DB
"""

from pydantic import BaseModel


class FileSecretCreationDto(BaseModel):
    """
    DTO wrapping a base64 encoded representation of a file encryption/decryption secret.
    Call site needs to handle encoding/decoding.
    """

    file_secret: str


class FileSecretDto(FileSecretCreationDto):
    """
    FileSecretCreationDto with added ID for file secrets returned from MongoDB
    """

    id: str
