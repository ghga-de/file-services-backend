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

"""DAO translators for accessing the database."""

from hexkit.protocols.dao import DaoFactoryProtocol
from hexkit.protocols.daopub import DaoPublisher, DaoPublisherFactoryProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from ucs.constants import (
    FILE_UPLOAD_BOXES_COLLECTION,
    FILE_UPLOADS_COLLECTION,
    S3_UPLOAD_DETAILS_COLLECTION,
)
from ucs.core import models
from ucs.ports.outbound.dao import S3UploadDetailsDao, UploadDaoPublisherFactoryPort


async def get_s3_upload_details_dao(
    *, dao_factory: DaoFactoryProtocol
) -> S3UploadDetailsDao:
    """Produce as S3UploadDetailsDao"""
    return await dao_factory.get_dao(
        name=S3_UPLOAD_DETAILS_COLLECTION,
        dto_model=models.S3UploadDetails,
        id_field="file_id",
    )


class UploadDaoConfig(BaseSettings):
    """Topic configuration for the published DTOs"""

    # TODO: Split this out and move to ghga-event-schemas.configs.stateful later on
    file_upload_box_topic: str = Field(
        ...,
        description="Topic containing published FileUploadBox outbox events",
        examples=["file-upload-boxes", "file-upload-box-topic"],
    )
    file_upload_topic: str = Field(
        ...,
        description="Topic containing published FileUpload outbox events",
        examples=["file-uploads", "file-upload-topic"],
    )


class UploadDaoPublisherFactory(UploadDaoPublisherFactoryPort):
    """Translation between UploadDaoPublisherFactoryPort and DaoPublisherFactoryProtocol."""

    def __init__(
        self,
        *,
        config: UploadDaoConfig,
        dao_publisher_factory: DaoPublisherFactoryProtocol,
    ):
        self._file_upload_box_topic = config.file_upload_box_topic
        self._file_upload_topic = config.file_upload_topic
        self._dao_publisher_factory = dao_publisher_factory

    # TODO: See if lambdas are sufficient for dto_to_event

    async def get_file_upload_box_dao(self) -> DaoPublisher[models.FileUploadBox]:
        """Construct an outbox DAO for FileUploadBox objects"""
        return await self._dao_publisher_factory.get_dao(
            name=FILE_UPLOAD_BOXES_COLLECTION,
            id_field="id",
            dto_model=models.FileUploadBox,
            dto_to_event=lambda x: x.model_dump(),
            event_topic=self._file_upload_box_topic,
            autopublish=True,
        )

    async def get_file_upload_dao(self) -> DaoPublisher[models.FileUpload]:
        """Construct an outbox DAO for FileUpload objects"""
        return await self._dao_publisher_factory.get_dao(
            name=FILE_UPLOADS_COLLECTION,
            id_field="id",
            dto_model=models.FileUpload,
            dto_to_event=lambda x: x.model_dump(),
            event_topic=self._file_upload_topic,
            autopublish=True,
        )
