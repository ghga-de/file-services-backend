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

"""Main business-logic of this service"""

from typing import Any, Callable, Dict, Optional

from ..config import CONFIG, Config
from ..dao import Database, DrsObjectNotFoundError, ObjectNotFoundError, ObjectStorage
from ..models import AccessMethod, AccessURL, Checksum, DrsObjectBase, DrsObjectServe


def get_drs_object_serve(
    drs_id: str,
    make_stage_request: Callable[[DrsObjectBase, Config], None],
    config: Config = CONFIG,
) -> Optional[DrsObjectServe]:
    """
    Gets the drs object for serving, if it exists in the outbox
    """

    with Database(config=config) as database:
        try:
            db_object_info = database.get_drs_object(drs_id)
        except DrsObjectNotFoundError:  # pylint: disable=try-except-raise
            raise

    # If object exists in Database, see if it exists in outbox

    bucket_id = config.s3_outbox_bucket_id

    with ObjectStorage(config=config) as storage:

        if storage.does_object_exist(bucket_id, drs_id):

            # create presigned url
            download_url = storage.get_object_download_url(bucket_id, drs_id)

            # return DRS Object
            return DrsObjectServe(
                file_id=drs_id,
                self_uri=f"{config.drs_self_url}/{drs_id}",
                size=db_object_info.size,
                created_time=db_object_info.creation_date.isoformat(),
                updated_time=db_object_info.creation_date.isoformat(),
                checksums=[Checksum(checksum=db_object_info.md5_checksum, type="md5")],
                access_methods=[
                    AccessMethod(access_url=AccessURL(url=download_url), type="s3")
                ],
            )

    # If the object does not exist, make a stage request
    make_stage_request(
        db_object_info,
        config,
    )

    return None


def handle_registered_file(
    drs_object: DrsObjectBase,
    publish_object_registered: Callable[[DrsObjectBase, Config], None],
    config: Config = CONFIG,
):
    """
    Add a new entry, based on the processed message, to the database and then publish
    a message that we did so
    """

    # write file entry to database
    with Database(config=config) as database:
        database.register_drs_object(drs_object)

    # publish message that the drs file has been registered
    publish_object_registered(drs_object, config)


def handle_staged_file(message: Dict[str, Any], config: Config = CONFIG):
    """
    Check if the file really is in the outbox,
    if it is, update properties in storage
    otherwise throw an error
    """

    file_id = message["file_id"]
    md5_checksum = message["md5_checksum"]

    # Check if file exists in database
    with Database(config=config) as database:
        db_object_info = database.get_drs_object(file_id)

        # Check if file is in outbox
        with ObjectStorage(config=config) as storage:
            if storage.does_object_exist(config.s3_outbox_bucket_id, file_id):

                db_object_info.md5_checksum = md5_checksum

                # Update information, in case something has changed
                database.update_drs_object(drs_object=db_object_info)
                return

            # Throw error, if the file does not exist in the outbox
            raise ObjectNotFoundError(
                object_id=file_id,
                bucket_id=config.s3_outbox_bucket_id,
            )
