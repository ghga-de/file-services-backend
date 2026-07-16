# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Integration tests for the MongoDbBoxStatsAggregator"""

from uuid import UUID, uuid4

import httpx
import pytest
from ghga_event_schemas.pydantic_ import InterrogationFailure
from hexkit.correlation import set_correlation_id
from hexkit.utils import now_utc_ms_prec

from tests_ucs.fixtures import utils
from tests_ucs.fixtures.joint import JointFixture
from ucs.constants import COUNTED_UPLOAD_STATES
from ucs.core.controller import UploadController

pytestmark = pytest.mark.asyncio()

NOT_COUNTED_UPLOAD_STATES = ("init", "failed", "cancelled")

CONTENT = "a" * 10 * 1024 * 1024  # 10 MiB
ENCRYPTED_SIZE = len(CONTENT)
DECRYPTED_SIZE = ENCRYPTED_SIZE - 124


async def setup_box_with_completed_upload(
    joint_fixture: JointFixture,
) -> tuple[UUID, UUID]:
    """Create a box and run a file upload of size DECRYPTED_SIZE through completion.

    Returns the box ID and file ID as a 2-tuple.
    """
    controller = joint_fixture.upload_controller
    async with set_correlation_id(uuid4()):
        box_id = await controller.create_file_upload_box(
            storage_alias="test", max_size=utils.TEST_MAX_BOX_SIZE
        )
        file_id, _ = await controller.initiate_file_upload(
            box_id=box_id,
            alias="test-file",
            decrypted_size=DECRYPTED_SIZE,
            encrypted_size=ENCRYPTED_SIZE,
            part_size=utils.PART_SIZE,
        )

    url = await controller.get_part_upload_url(file_id=file_id, part_no=1)
    assert httpx.put(url, content=CONTENT).status_code == 200

    async with set_correlation_id(uuid4()):
        await controller.complete_file_upload(
            box_id=box_id,
            file_id=file_id,
            unencrypted_checksum="abc123",
            encrypted_checksum=utils.calc_expected_encrypted_checksum(CONTENT),
            encrypted_parts_md5=["abc123"],
            encrypted_parts_sha256=["def456"],
        )
    return box_id, file_id


async def fail_upload(joint_fixture: JointFixture, *, file_id: UUID) -> None:
    """Process an interrogation failure for the given completed upload."""
    report = InterrogationFailure(
        file_id=file_id,
        storage_alias="test",
        interrogated_at=now_utc_ms_prec(),
        reason="Decryption failed",
    )
    async with set_correlation_id(uuid4()):
        await joint_fixture.upload_controller.process_interrogation_failure(
            report=report
        )


async def test_box_stats_aggregation(joint_fixture: JointFixture):
    """Test MongoDbBoxStatsAggregator.compute_box_stats against a real MongoDB instance.

    The FileUpload documents are written through the DAO so that the docs in the DB
    more reliably have the correct structure, e.g. __metadata__, field encoding, etc.
    """
    controller = joint_fixture.upload_controller
    assert isinstance(controller, UploadController)
    box_stats_aggregator = controller._box_stats_aggregator
    file_upload_dao = controller._file_upload_dao

    box_id = uuid4()
    other_box_id = uuid4()

    # Try calculating stats box without any FileUploads
    assert await box_stats_aggregator.compute_box_stats(box_id=box_id) == (0, 0)

    # Insert one FileUpload per state, each with a distinct decrypted_size
    counted_sizes_by_file_id: dict[UUID, int] = {}
    not_counted_size_total = 0
    async with set_correlation_id(uuid4()):
        for index, state in enumerate(
            COUNTED_UPLOAD_STATES + NOT_COUNTED_UPLOAD_STATES
        ):
            file_upload = utils.make_file_upload(state=state)
            file_upload.box_id = box_id
            file_upload.alias = f"file_{state}"
            file_upload.decrypted_size = 100 * (index + 1)
            await file_upload_dao.insert(file_upload)
            if state in COUNTED_UPLOAD_STATES:
                counted_sizes_by_file_id[file_upload.id] = file_upload.decrypted_size
            else:
                not_counted_size_total += file_upload.decrypted_size

        # Also insert a 'counted' FileUpload into a different box
        other_box_file_upload = utils.make_file_upload(state="inbox")
        other_box_file_upload.box_id = other_box_id
        await file_upload_dao.insert(other_box_file_upload)

    # Only the counted states of the requested box may contribute to the stats
    assert not_counted_size_total > 0
    file_count, total_size = await box_stats_aggregator.compute_box_stats(box_id=box_id)
    assert file_count == len(counted_sizes_by_file_id)
    assert total_size == sum(counted_sizes_by_file_id.values())

    # Make sure calc only considers files owned by the given box
    file_count, total_size = await box_stats_aggregator.compute_box_stats(
        box_id=other_box_id
    )
    assert file_count == 1
    assert total_size == other_box_file_upload.decrypted_size

    # Make sure 'deleted' outbox docs are not counted somehow
    deleted_file_id, _ = counted_sizes_by_file_id.popitem()
    async with set_correlation_id(uuid4()):
        await file_upload_dao.delete(deleted_file_id)
    file_count, total_size = await box_stats_aggregator.compute_box_stats(box_id=box_id)
    assert file_count == len(counted_sizes_by_file_id)
    assert total_size == sum(counted_sizes_by_file_id.values())


async def test_cancelled_upload_excluded_from_stats(joint_fixture: JointFixture):
    """Test that a cancelled upload is excluded from box stats even though its
    FileUpload document remains in the DB. This is tested obliquely in the test above,
    but more explicitly here.
    """
    controller = joint_fixture.upload_controller
    assert isinstance(controller, UploadController)
    box_stats_aggregator = controller._box_stats_aggregator

    box_id, file_id = await setup_box_with_completed_upload(joint_fixture)
    assert await box_stats_aggregator.compute_box_stats(box_id=box_id) == (
        1,
        DECRYPTED_SIZE,
    )

    async with set_correlation_id(uuid4()):
        await controller.remove_file_upload(box_id=box_id, file_id=file_id)

    cancelled_upload = await controller._file_upload_dao.get_by_id(file_id)
    assert cancelled_upload.state == "cancelled"
    assert await box_stats_aggregator.compute_box_stats(box_id=box_id) == (0, 0)


async def test_interrogation_failed_upload_still_counted(joint_fixture: JointFixture):
    """Test that a file that failed interrogation still counts toward box stats."""
    controller = joint_fixture.upload_controller
    assert isinstance(controller, UploadController)
    box_stats_aggregator = controller._box_stats_aggregator

    box_id, file_id = await setup_box_with_completed_upload(joint_fixture)
    await fail_upload(joint_fixture, file_id=file_id)

    failed_upload = await controller._file_upload_dao.get_by_id(file_id)
    assert failed_upload.state == "failed"
    assert await box_stats_aggregator.compute_box_stats(box_id=box_id) == (
        1,
        DECRYPTED_SIZE,
    )


async def test_failed_upload_retry_releases_stats(joint_fixture: JointFixture):
    """Ensure that when retrying a failed upload we don't see double counting or
    anything. The file count and total size should be correct.
    """
    controller = joint_fixture.upload_controller
    assert isinstance(controller, UploadController)
    box_stats_aggregator = controller._box_stats_aggregator

    box_id, file_id = await setup_box_with_completed_upload(joint_fixture)
    await fail_upload(joint_fixture, file_id=file_id)

    # Retry the upload for the same alias, replacing the failed one
    async with set_correlation_id(uuid4()):
        new_file_id, _ = await controller.initiate_file_upload(
            box_id=box_id,
            alias="test-file",
            decrypted_size=DECRYPTED_SIZE,
            encrypted_size=ENCRYPTED_SIZE,
            part_size=utils.PART_SIZE,
        )
    assert new_file_id != file_id
    assert await box_stats_aggregator.compute_box_stats(box_id=box_id) == (0, 0)

    # Complete the retried upload and make sure it counts again
    url = await controller.get_part_upload_url(file_id=new_file_id, part_no=1)
    assert httpx.put(url, content=CONTENT).status_code == 200
    async with set_correlation_id(uuid4()):
        await controller.complete_file_upload(
            box_id=box_id,
            file_id=new_file_id,
            unencrypted_checksum="abc123",
            encrypted_checksum=utils.calc_expected_encrypted_checksum(CONTENT),
            encrypted_parts_md5=["abc123"],
            encrypted_parts_sha256=["def456"],
        )
    assert await box_stats_aggregator.compute_box_stats(box_id=box_id) == (
        1,
        DECRYPTED_SIZE,
    )
