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

"""Main business-logic of this service"""

import re

from pydantic import BaseSettings, Field, PositiveInt, validator

from dcs.adapters.outbound.http import exceptions
from dcs.adapters.outbound.http.api_calls import call_ekss_api
from dcs.core import models
from dcs.ports.inbound.data_repository import DataRepositoryPort
from dcs.ports.outbound.dao import DrsObjectDaoPort, ResourceNotFoundError
from dcs.ports.outbound.event_pub import EventPublisherPort
from dcs.ports.outbound.storage import ObjectStoragePort


class DataRepositoryConfig(BaseSettings):
    """Config parameters needed for the DataRepository."""

    outbox_bucket: str
    drs_server_uri: str = Field(
        ...,
        description="The base of the DRS URI to access DRS objects. Has to start with 'drs://'"
        + " and end with '/'.",
        example="drs://localhost:8080/",
    )
    retry_access_after: int = Field(
        120,
        description="When trying to access a DRS object that is not yet in the outbox, instruct"
        + " to retry after this many seconds.",
    )
    ekss_base_url: str = Field(
        ...,
        description="URL containing host and port of the EKSS endpoint to retrieve"
        + " personalized envelope from",
        example="http://ekss:8080/",
    )
    presigned_url_expires_after: PositiveInt = Field(
        ...,
        description="Expiration time in seconds for presigned URLS. Positive integer required",
        example=30,
    )

    # pylint: disable=no-self-argument
    @validator("drs_server_uri")
    def check_server_uri(cls, value: str):
        """Checks the drs_server_uri."""

        if not re.match(r"^drs://.+/$", value):
            raise ValueError(
                f"The drs_server_uri has to start with 'drs://' and end with '/', got : {value}"
            )

        return value


class DataRepository(DataRepositoryPort):
    """A service that manages a registry of DRS objects."""

    def __init__(
        self,
        *,
        config: DataRepositoryConfig,
        drs_object_dao: DrsObjectDaoPort,
        object_storage: ObjectStoragePort,
        event_publisher: EventPublisherPort,
    ):
        """Initialize with essential config params and outbound adapters."""

        self._config = config
        self._event_publisher = event_publisher
        self._drs_object_dao = drs_object_dao
        self._object_storage = object_storage

    def _get_drs_uri(self, *, drs_id: str) -> str:
        """Construct DRS URI for the given DRS ID."""

        return f"{self._config.drs_server_uri}{drs_id}"

    def _get_model_with_self_uri(
        self, *, drs_object: models.DrsObject
    ) -> models.DrsObjectWithUri:
        """Add the DRS self URI to an DRS object."""

        return models.DrsObjectWithUri(
            **drs_object.dict(),
            self_uri=self._get_drs_uri(drs_id=drs_object.file_id),
        )

    async def _get_access_model(
        self, *, drs_object: models.DrsObject
    ) -> models.DrsObjectWithAccess:
        """Get a DRS Object model with access information."""

        access_url = await self._object_storage.get_object_download_url(
            bucket_id=self._config.outbox_bucket,
            object_id=drs_object.file_id,
            expires_after=self._config.presigned_url_expires_after,
        )

        return models.DrsObjectWithAccess(
            **drs_object.dict(),
            self_uri=self._get_drs_uri(drs_id=drs_object.file_id),
            access_url=access_url,
        )

    async def access_drs_object(self, *, drs_id: str) -> models.DrsObjectResponseModel:
        """
        Serve the specified DRS object with access information.
        If it does not exists in the outbox, yet, a RetryAccessLaterError is raised that
        instructs to retry the call after a specified amount of time.
        """

        # make sure that metadata for the DRS object exists in the database:
        try:
            drs_object = await self._drs_object_dao.get_by_id(drs_id)
        except ResourceNotFoundError as error:
            raise self.DrsObjectNotFoundError(drs_id=drs_id) from error

        drs_object_with_uri = self._get_model_with_self_uri(drs_object=drs_object)

        # check if the file corresponding to the DRS object is already in the outbox:
        if not await self._object_storage.does_object_exist(
            bucket_id=self._config.outbox_bucket, object_id=drs_object.file_id
        ):
            # publish an event to request a stage of the corresponding file:
            await self._event_publisher.unstaged_download_requested(
                drs_object=drs_object_with_uri
            )

            # instruct to retry later:
            raise self.RetryAccessLaterError(
                retry_after=self._config.retry_access_after
            )
        drs_object_with_access = await self._get_access_model(drs_object=drs_object)

        # publish an event indicating the served download:
        await self._event_publisher.download_served(drs_object=drs_object_with_uri)

        return drs_object_with_access.convert_to_drs_response_model()

    async def register_new_file(self, *, file: models.DrsObject):
        """Register a file as a new DRS Object."""

        # write file entry to database
        await self._drs_object_dao.insert(file)

        # publish message that the drs file has been registered
        drs_object_with_uri = self._get_model_with_self_uri(drs_object=file)
        await self._event_publisher.file_registered(drs_object=drs_object_with_uri)

    async def serve_envelope(self, *, drs_id: str, public_key: str) -> str:
        """
        Retrieve envelope for the object with the given DRS ID

        :returns: base64 encoded envelope bytes
        """

        try:
            drs_object = await self._drs_object_dao.get_by_id(id_=drs_id)
        except ResourceNotFoundError as error:
            raise self.DrsObjectNotFoundError(drs_id=drs_id) from error

        try:
            envelope = call_ekss_api(
                secret_id=drs_object.decryption_secret_id,
                receiver_public_key=public_key,
                api_base=self._config.ekss_base_url,
            )
        except (
            exceptions.BadResponseCodeError,
            exceptions.RequestFailedError,
        ) as error:
            raise self.APICommunicationError(
                api_url=self._config.ekss_base_url
            ) from error
        except exceptions.SecretNotFoundError as error:
            raise self.EnvelopeNotFoundError(object_id=drs_id) from error

        return envelope
