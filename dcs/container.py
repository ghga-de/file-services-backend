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

"""Module hosting the dependency injection container."""

from hexkit.inject import ContainerBase, get_configurator, get_constructor
from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber
from hexkit.providers.mongodb import MongoDbDaoFactory

from dcs.adapters.inbound.event_sub import EventSubTranslator
from dcs.adapters.outbound.dao import DrsObjectDaoConstructor
from dcs.adapters.outbound.event_pub import EventPubTranslator
from dcs.adapters.outbound.s3 import S3ObjectStorage
from dcs.config import Config
from dcs.core.data_repository import DataRepository


class Container(ContainerBase):
    """DI Container"""

    config = get_configurator(Config)

    # outbound providers:
    dao_factory = get_constructor(MongoDbDaoFactory, config=config)
    event_pub_provider = get_constructor(KafkaEventPublisher, config=config)

    # outbound translators:
    drs_object_dao = get_constructor(DrsObjectDaoConstructor, dao_factory=dao_factory)
    event_publisher = get_constructor(
        EventPubTranslator, config=config, provider=event_pub_provider
    )

    # outbound adapters (not following the triple hexagonal translator/provider)
    object_storage = get_constructor(S3ObjectStorage, config=config)

    # domain/core components:
    data_repository = get_constructor(
        DataRepository,
        drs_object_dao=drs_object_dao,
        object_storage=object_storage,
        event_publisher=event_publisher,
        config=config,
    )

    # inbound translators:
    event_sub_translator = get_constructor(
        EventSubTranslator,
        data_repository=data_repository,
        config=config,
    )

    # inbound providers:
    event_subscriber = get_constructor(
        KafkaEventSubscriber, config=config, translator=event_sub_translator
    )
