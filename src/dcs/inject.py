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

"""Module hosting the dependency injection container."""

from collections.abc import AsyncGenerator, Coroutine
from contextlib import asynccontextmanager
from typing import Callable, Optional

from fastapi import FastAPI
from ghga_service_commons.api import configure_app
from ghga_service_commons.auth.jwt_auth import JWTAuthContextProvider
from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber
from hexkit.providers.mongodb import MongoDbDaoFactory
from typing_extensions import TypeAlias

from dcs.adapters.inbound.event_sub import EventSubTranslator
from dcs.adapters.inbound.fastapi_ import dummies
from dcs.adapters.inbound.fastapi_.custom_openapi import get_openapi_schema
from dcs.adapters.inbound.fastapi_.routes import router
from dcs.adapters.outbound.dao import DrsObjectDaoConstructor
from dcs.adapters.outbound.event_pub import EventPubTranslator
from dcs.adapters.outbound.s3 import S3ObjectStorage
from dcs.config import Config
from dcs.core.auth_policies import WorkOrderContext
from dcs.core.data_repository import DataRepository
from dcs.ports.inbound.data_repository import DataRepositoryPort
from dcs.utils import asyncnullcontext


@asynccontextmanager
async def prepare_core(*, config: Config) -> AsyncGenerator[DataRepositoryPort, None]:
    """Constructs and initializes all core components and their outbound dependencies.
    This is a context manager returns a container with all initialized resources upon
    __enter__. The resources are teared down upon __exit__.

    """
    dao_factory = MongoDbDaoFactory(config=config)
    drs_object_dao = await DrsObjectDaoConstructor.construct(dao_factory=dao_factory)
    object_storage = S3ObjectStorage(config=config)

    async with KafkaEventPublisher.construct(config=config) as event_pub_provider:
        event_publisher = EventPubTranslator(config=config, provider=event_pub_provider)

        yield DataRepository(
            drs_object_dao=drs_object_dao,
            object_storage=object_storage,
            event_publisher=event_publisher,
            config=config,
        )


OutboxCleaner: TypeAlias = Callable[[], Coroutine[None, None, None]]


def get_configured_rest_app(*, config: Config) -> FastAPI:
    """Create and configure a REST API application."""
    app = FastAPI()
    app.include_router(router)
    configure_app(app, config=config)

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi_schema(app)
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore [method-assign]

    return app


@asynccontextmanager
async def prepare_rest_app(
    *,
    config: Config,
    data_repo_override: Optional[DataRepositoryPort] = None,
) -> AsyncGenerator[FastAPI, None]:
    """Construct and initialize an REST API app along with all its dependencies.
    By default, the core dependencies are automatically prepared but you can also
    provide them using the data_repo_override parameter.
    """
    app = get_configured_rest_app(config=config)

    data_repository_cm = (
        asyncnullcontext(data_repo_override)
        if data_repo_override
        else prepare_core(config=config)
    )

    async with (
        data_repository_cm as data_repository,
        JWTAuthContextProvider.construct(
            config=config, context_class=WorkOrderContext
        ) as auth_context,
    ):
        app.dependency_overrides[dummies.auth_provider] = lambda: auth_context
        app.dependency_overrides[dummies.data_repo_port] = lambda: data_repository
        yield app


@asynccontextmanager
async def prepare_event_subscriber(
    *,
    config: Config,
    data_repo_override: Optional[DataRepositoryPort] = None,
) -> AsyncGenerator[KafkaEventSubscriber, None]:
    """Construct and initialize an event subscriber with all its dependencies.
    By default, the core dependencies are automatically prepared but you can also
    provide them using the data_repo_override parameter.
    """
    data_repository_cm = (
        asyncnullcontext(data_repo_override)
        if data_repo_override
        else prepare_core(config=config)
    )

    async with data_repository_cm as data_repository:
        event_sub_translator = EventSubTranslator(
            data_repository=data_repository,
            config=config,
        )

        async with KafkaEventSubscriber.construct(
            config=config, translator=event_sub_translator
        ) as event_subscriber:
            yield event_subscriber


@asynccontextmanager
async def prepare_outbox_cleaner(
    *,
    config: Config,
    data_repo_override: Optional[DataRepositoryPort] = None,
) -> AsyncGenerator[OutboxCleaner, None]:
    """Construct and initialize a coroutine that cleans the outbox once invoked.
    By default, the core dependencies are automatically prepared but you can also
    provide them using the data_repo_override parameter.
    """
    data_repository_cm = (
        asyncnullcontext(data_repo_override)
        if data_repo_override
        else prepare_core(config=config)
    )

    async with data_repository_cm as data_repository:
        yield data_repository.cleanup_outbox
