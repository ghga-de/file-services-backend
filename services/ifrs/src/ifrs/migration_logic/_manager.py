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
"""Tools to run database migrations in services"""

import logging
from contextlib import asynccontextmanager
from time import sleep, time
from typing import Any, Literal

from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.providers.mongodb import MongoDbConfig
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel, Field
from pymongo.errors import DuplicateKeyError

from ._utils import MigrationDefinition, Reversible

log = logging.getLogger(__name__)

MigrationType = Literal["FORWARD", "BACKWARD"]
MigrationCls = type[MigrationDefinition]
MigrationMap = dict[int, MigrationCls]


def duration_in_ms(duration: float) -> int:
    return int(duration * 1000)


class MigrationConfig(MongoDbConfig):
    """Minimal configuration required to run the migration process."""

    lock_collection: str = Field(
        ...,
        description="The name of the collection containing the DB Lock document for this service",
        examples=["ifrsLock", "dcsLock"],
    )
    db_version_collection: str = Field(
        ...,
        description="The name of the collection containing DB version information for this service",
        examples=["ifrsDbVersions"],
    )
    migration_wait_sec: int = Field(
        ...,
        description="The number of seconds to wait before checking the DB version again",
        examples=[5, 30, 180],
    )


class DbVersionRecord(BaseModel):
    """Model containing information about DB versions and how they were achieved."""

    version: int = Field(..., description="The database version")
    details: dict[str, Any] = Field(
        ...,
        description="Extra information about the migration to this version. ",
        examples=[
            {
                "completed": "2025-01-17T13:42:58.396538+00:00",
                "migration_type": "FORWARD",
                "total_duration_ms": 5000,
            }
        ],
    )


class MigrationStepError(RuntimeError):
    """Raised when a specific migration step fails, e.g. migrating from v4 to v5"""

    def __init__(self, *, current_ver: int, target_ver: int, err_info: str):
        msg = (
            f"Unable to migrate from DB version {current_ver} to {target_ver}."
            + f" Cause:\n  '{err_info}'"
        )
        super().__init__(msg)


class DbLockError(RuntimeError):
    """Raised when the DB lock can't be released or acquired due to an error."""

    def __init__(
        self, *, op: Literal["acquire", "release"], coll_name: str, err_info: str
    ):
        msg = (
            f"Failed to {op} the lock in DB lock collection {coll_name}."
            + f" Error details:\n  '{err_info}'"
        )
        super().__init__(msg)


class DbVersioningInitError(RuntimeError):
    """Raised when DB versioning initialization fails due to an error."""

    def __init__(self, *, err_info: str):
        msg = f"DB versioning initialization failed. Error details:\n  {err_info}"
        super().__init__(msg)


def _get_db_version_from_records(version_docs: list[DbVersionRecord]) -> int:
    """Gets the current DB version from the documents found in the version collection."""
    # Make sure we know what the latest version is, not just the max
    version_docs.sort(key=lambda doc: doc.details["completed"])
    return version_docs[-1].version if version_docs else 0


class MigrationManager:
    """Top-level logic for ensuring the database is updated before running the service.

    The `migrate_or_wait` method must be called before any instance of the service
    begins its main execution loop.

    Reserved DB version numbers:
    - 0 = db versioning not yet implemented
    - 1 = db versioning implemented

    Example usage:
    ```
    from my_service.config import Config  # inherits from MongoDbConfig
    from ghga_service_commons.migration import MigrationManager, MigrationMap

    DB_VERSION = 2  # the current expected DB version
    MY_MIGRATION_MAP = MigrationMap({
        2: V2Migration,
        # future migrations will go here
    })

    def migrate_my_service():
        # Called before starting my_service
        config = Config()

        with MigrationManager(config, DB_VERSION, MY_MIGRATION_MAP) as mm:
            mm.migrate_or_wait()
    ```
    """

    client: AsyncIOMotorClient
    db: AsyncIOMotorDatabase

    def __init__(
        self,
        config: MigrationConfig,
        target_version: int,
        migration_map: MigrationMap,
    ):
        """Instantiate the MigrationManager.

        Args
        - `config`: Config containing db connection str and lock/db versioning collections
        - `target_version`: Which version the db needs to be at for this version of the service
        - `migration_map`: A dict with the MigrationDefinition class for each db version
        """
        if target_version < 1:
            raise RuntimeError("Expected database version must be 1 or greater")

        self.config = config
        self.target_ver = target_version
        self.migration_map = migration_map
        self._lock_acquired = False
        self._entered = False
        self._migration_type: MigrationType = "FORWARD"

    async def __aenter__(self):
        """Set up database client and database reference"""
        self.client = AsyncIOMotorClient(
            self.config.db_connection_str.get_secret_value()
        )
        self.db = self.client[self.config.db_name]
        self._entered = True
        return self

    async def __aexit__(self, exc_type_, exc_value, exc_tb):
        """Release DB lock and close/remove database client"""
        await self._release_db_lock()
        self.client.close()

    async def _get_version_docs(self) -> list[DbVersionRecord]:
        """Gets the DB version information from the database."""
        collection = self.db[self.config.db_version_collection]
        version_docs = [DbVersionRecord(**doc) async for doc in collection.find()]

        return version_docs

    @asynccontextmanager
    async def _lock_db(self):
        await self._acquire_db_lock()
        try:
            yield
        finally:
            await self._release_db_lock()

    async def _acquire_db_lock(self):
        """Try to acquire the lock on the DB and return the result.

        Logs and raises any error that occurs while updating the lock document.
        """
        if self._lock_acquired:
            log.debug("Database lock already acquired")
            return

        try:
            lock_col = self.db[self.config.lock_collection]
            lock_acquired = await lock_col.find_one_and_update(
                {"lock_acquired": False},
                {
                    "$set": {
                        "lock_acquired": True,
                        "acquired_at": now_as_utc().isoformat(),
                    }
                },
            )
        except BaseException as exc:
            error = DbLockError(
                op="acquire", coll_name=self.config.lock_collection, err_info=str(exc)
            )
            log.error(error)
            raise error from exc

        self._lock_acquired = bool(lock_acquired)
        if self._lock_acquired:
            log.info("Database lock acquired")

    async def _release_db_lock(self) -> None:
        """Release the DB lock.

        Logs and re-raises any errors that occur during the update.
        """
        if not self._lock_acquired:
            log.debug("Database lock already released")
            return
        try:
            lock_col = self.db[self.config.lock_collection]
            await lock_col.find_one_and_update(
                {"lock_acquired": True},
                {"$set": {"lock_acquired": False, "acquired_at": ""}},
            )
            self._lock_acquired = False
        except BaseException as exc:
            error = DbLockError(
                op="release",
                coll_name=self.config.lock_collection,
                err_info=str(exc),
            )
            log.critical(error)
            raise error from exc
        log.info("Database lock released")

    async def _record_migration(self, *, version: int, total_duration_ms: int):
        """Insert a DbVersionRecord with processing information"""
        details: dict[str, Any] = {
            "completed": now_as_utc().isoformat(),
            "total_duration_ms": total_duration_ms,
            "migration_type": self._migration_type,
        }
        record = DbVersionRecord(version=version, details=details)
        version_collection = self.db[self.config.db_version_collection]
        await version_collection.insert_one(record.model_dump())

    async def _initialize_versioning(self) -> bool:
        """Create and acquire the DB lock, then add the versioning collection.

        Returns `True` if setup was performed, else `False`.
        """
        init_start = time()
        lock_collection = self.db[self.config.lock_collection]
        lock_doc = [_ async for _ in lock_collection.find()]
        if not lock_doc:
            # lock document has not been created yet, so add it
            try:
                await lock_collection.insert_one(
                    {
                        "_id": 0,
                        "lock_acquired": False,
                        "acquired_at": "",
                    }
                )
            except DuplicateKeyError:
                # another instance inserted the doc first, so stop and wait to retry
                return False

        # Lock database so other instances can't attempt migrations
        async with self._lock_db():
            if not self._lock_acquired:
                return False

            # Initialize db version collection
            await self._record_migration(
                version=1,
                total_duration_ms=duration_in_ms(time() - init_start),
            )
        return True

    def _get_sequence(self, *, current_ver: int, target_ver: int) -> list[int]:
        """Return an ordered list of the version migrations to apply/unapply"""
        # In forward case, we don't need to apply current ver
        # in backward case, we don't want to unapply the target ver
        step_range = (
            range(current_ver, target_ver, -1)
            if self._migration_type == "BACKWARD"
            else range(current_ver + 1, target_ver + 1)
        )
        steps = list(step_range)
        return steps

    def _fetch_migration_cls(self, version: int) -> MigrationCls:
        """Return the stored migration for the specified version.

        Raise an error if the  doesn't exist or doesn't implement unapply when needed.
        """
        try:
            migration_cls = self.migration_map[version]
            if self._migration_type == "BACKWARD" and not issubclass(
                migration_cls, Reversible
            ):
                raise RuntimeError(
                    f"Planning to unapply migration v{version}, but"
                    + f" it doesn't subclass `{Reversible.__name__}`!"
                )
            return migration_cls
        except KeyError as err:
            mig_type = self._migration_type.lower()
            raise NotImplementedError(
                f"No {mig_type} migration implemented for version {version}"
            ) from err

    async def _perform_migrations(self, *, current_ver: int):
        """Migrate forward or backward to reach target DB version.

        Raises `MigrationError` if unsuccessful.
        """
        seq = self._get_sequence(current_ver=current_ver, target_ver=self.target_ver)
        migrations = [self._fetch_migration_cls(ver) for ver in seq]
        unapplying = self._migration_type == "BACKWARD"
        try:
            # Execute & time each migration in order to get to the target DB version
            for v, migration_cls in zip(seq, migrations, strict=True):
                # Determine if this is the last migration to apply/unapply
                last_ver_called = self.target_ver + 1 if unapplying else self.target_ver
                is_final_migration = v == last_ver_called

                # instantiate MigrationDefinition
                migration = migration_cls(
                    db=self.db,
                    unapplying=unapplying,
                    is_final_migration=is_final_migration,
                )

                # Call apply/unapply based on migration type
                await migration.unapply() if unapplying else await migration.apply()
        except BaseException as exc:
            error = MigrationStepError(
                current_ver=current_ver, target_ver=self.target_ver, err_info=str(exc)
            )
            log.critical(error)
            raise error from exc

    async def _migrate_db(self) -> bool | None:
        """Ensure the database is up to date before running the actual app.

        If the database is already up to date, no changes are made. If the database is
        out of date, migration code is executed to make the database current.

        Returns True if migrations are finished or up-to-date and False otherwise.
        """
        version_docs = await self._get_version_docs()
        version = _get_db_version_from_records(version_docs)

        if version == 0:
            try:
                init_complete = await self._initialize_versioning()
            except BaseException as exc:
                error = DbVersioningInitError(err_info=str(exc))
                log.critical(error)
                raise error from exc
            if not init_complete:
                return False
            version = 1

        if version == self.target_ver:
            # DB is up to date, run service
            return True

        # DB version is not what it should be: acquire lock and migrate
        async with self._lock_db():
            if not self._lock_acquired:
                return False

            self._migration_type = (
                "FORWARD" if version < self.target_ver else "BACKWARD"
            )

            start = time()
            await self._perform_migrations(current_ver=version)
            duration_ms = duration_in_ms(time() - start)

            # record the db version
            await self._record_migration(
                version=self.target_ver,
                total_duration_ms=duration_ms,
            )
        return True

    async def migrate_or_wait(self):
        """Try to migrate the database or wait until migrations are completed."""
        if not self._entered:
            raise RuntimeError("MigrationManager must be used as a context manager")

        # need to implement some kind of total time limit, warning logging, etc. later
        while not await self._migrate_db():
            sleep(self.config.migration_wait_sec)
