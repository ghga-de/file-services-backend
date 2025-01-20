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
"""Utils for defining and applying database migrations"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from copy import deepcopy
from typing import Any

from hexkit.providers.mongodb.provider import document_to_dto, dto_to_document
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel

log = logging.getLogger(__name__)

Document = dict[str, Any]


def validate_doc(doc: Document, *, model: type[BaseModel], id_field: str):
    """Ensure that new content passes model validation.

    Also check that `dto_to_document` results in the same document.
    """
    as_model = document_to_dto(deepcopy(doc), id_field=id_field, dto_model=model)
    doc_from_model = dto_to_document(as_model, id_field=id_field)
    if doc != doc_from_model:
        raise RuntimeError(
            f"Doc validation failed for model '{model.__name__}',"
            + f" expected: {str(doc_from_model)}, but got {doc}"
        )


class MigrationDefinition:
    """Contains all logic to migrate the database from one version to the next."""

    version: int

    def __init__(
        self,
        *,
        db: AsyncIOMotorDatabase,
        unapplying: bool,
        is_final_migration: bool,
    ):
        """Instantiate the MigrationDefinition.

        Subclass overrides need to call `super().__init__` or include its code.
        """
        if not self.version:
            raise ValueError("Migration version has not been assigned")

        self._db = db
        unapply = "_unapply" if unapplying else ""
        self._temp_prefix = f"tmp_v{self.version}{unapply}"
        self._new_prefix = f"{self._temp_prefix}_new"
        self._old_prefix = f"{self._temp_prefix}_old"
        self._is_final_migration = is_final_migration
        self._indexes_applied = False
        self._collections_staged = False
        self._log_blurb = f"for {'downgrade' if unapplying else 'upgrade'} to DB version {self.version}"

    @staticmethod
    def _add_prefix(name: str, prefix: str) -> str:
        """Adds a prefix to a string or returns the string unchanged."""
        if name.startswith(prefix):
            return name
        return f"{prefix}_{name}"

    def new_temp_name(self, coll_name: str) -> str:
        """Add `self._new_prefix` to a plain collection name."""
        return self._add_prefix(coll_name, self._new_prefix)

    def get_prefixed_old_name(self, coll_name: str) -> str:
        """Add `self._old_prefix` to plain collection name."""
        return self._add_prefix(coll_name, self._old_prefix)

    def get_new_temp_names(self, coll_name: list[str]) -> list[str]:
        """Add `self._new_prefix` to a list of plain collection names."""
        return [self.new_temp_name(name) for name in coll_name]

    def get_old_temp_names(self, coll_name: list[str]) -> list[str]:
        """Add `self._old_prefix` to a list of plain collection names."""
        return [self.get_prefixed_old_name(name) for name in coll_name]

    async def migrate_docs_in_collection(
        self,
        *,
        coll_name: str,
        change_function: Callable[[Document], Awaitable[Document]],
        validation_model: type[BaseModel] | None = None,
        id_field: str = "",
        force_validate: bool = False,
    ):
        """Migrate a collection by calling `change_function` on each document within.

        If `validation_model` is supplied, model will be used to cross-check the
        resulting doc data when this is the last migration to be applied/unapplied OR
        `always_validate` is True.
        """
        if self._collections_staged:
            raise RuntimeError("Collections already staged, changes shouldn't be made.")

        old_collection = self._db[coll_name]
        method = change_function

        temp_new_coll_name = self.new_temp_name(coll_name)
        temp_new_collection = self._db[temp_new_coll_name]

        # naive implementation - update to use batching and bulk inserts
        async for doc in old_collection.find():
            output_doc = await method(doc)

            # do validation against model only if we're on the last migration because
            # the model defined in code may is not guaranteed to match until that time
            if validation_model and (self._is_final_migration or force_validate):
                validate_doc(output_doc, model=validation_model, id_field=id_field)

            # insert into new collection
            await temp_new_collection.insert_one(output_doc)
        log.debug("Changes applied to collection '%s' %s", coll_name, self._log_blurb)

    async def stage_collection(self, coll_name: str):
        """Stage a single collection"""
        old_collection = self._db[coll_name]
        temp_old_coll_name = self.get_prefixed_old_name(coll_name)
        await old_collection.rename(temp_old_coll_name)

        temp_new_coll_name = self.new_temp_name(coll_name)
        new_collection = self._db[temp_new_coll_name]
        await new_collection.rename(coll_name)
        log.debug("Staged changes for collection %s", coll_name)

    async def stage_new_collections(self, coll_names: str | list[str]):
        """Rename old collections to temporarily move them aside without dropping them.
        Remove temporary prefix from updated collections.

        Assumes apply or unapply has completed.
        """
        if isinstance(coll_names, str):
            coll_names = [coll_names]
        for coll_name in coll_names:
            await self.stage_collection(coll_name)
        self._collections_staged = True
        log.info("Temp collections staged %s", self._log_blurb)

    async def copy_indexes(self, coll_names: list[str]):
        """Copy the indexes from old collections to new."""
        # self._indexes_applied = True
        # log.info("Indexes copied to new collections %s", self._log_blurb)
        raise NotImplementedError()

    async def drop_old_collections(
        self, coll_names: str | list[str], enforce_indexes: bool
    ):
        """Drop collections.

        Args
        - `coll_names`: list of the original un-prefixed collection names modified in
            this migration.
        - `enforce_indexes`: Raise an error if indexes haven't been copied over to the
            replacement collections. This is not always useful, since the collections
            might undergo changes that make old indexes obsolete. This should be set to
            True for modifications that don't involve changes to the collections' indexes.
        """
        if enforce_indexes and not self._indexes_applied:
            raise RuntimeError("Indexes have not been applied to migrated collections")
        if isinstance(coll_names, str):
            coll_names = [coll_names]
        for to_drop in [f"{self._old_prefix}_{coll_name}" for coll_name in coll_names]:
            collection = self._db[to_drop]
            await collection.drop()

    @abstractmethod
    async def apply(self):
        """Make the changes required to move the DB version to `self.version`."""
        ...

    async def unapply(self):
        """Placeholder for a method to reverse the migration changes.

        To implement, additionally subclass Reversible:

        ```
        class MyMigration(MigrationDefinition, Reversible):
            ...
        ```
        """
        raise NotImplementedError()


class Reversible(ABC):
    """Mixin class to mark a migration class as reversible."""

    @abstractmethod
    async def unapply(self):
        """Reverse changes made by `apply()`."""
        ...
