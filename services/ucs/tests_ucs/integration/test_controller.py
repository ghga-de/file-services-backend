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

"""Integration tests for the UploadController"""

from contextlib import nullcontext
from tempfile import NamedTemporaryFile
from uuid import UUID, uuid4

import httpx
import pytest
from ghga_service_commons.api.testing import AsyncTestClient
from hexkit.correlation import set_correlation_id

from tests_ucs.fixtures import utils
from tests_ucs.fixtures.joint import JointFixture
from ucs.inject import prepare_rest_app

pytestmark = pytest.mark.asyncio()


def auth_header(token: str) -> dict[str, str]:
    """Return auth header with the token embedded"""
    return {"Authorization": f"Bearer {token}"}


async def test_integrated_aspects(joint_fixture: JointFixture):
    """Test aspects that are not easily testable with unit test mocks:
    - outbox event publishing (e.g. the result of `dto_to_event`)
    - validity of returned s3 file part upload URL

    This also serves as a truncated happy path test. It will not test all actions, just
    some of the core behavior branches.
    """
    jwk = joint_fixture.jwk
    kafka = joint_fixture.kafka
    config = joint_fixture.config

    async with (
        prepare_rest_app(config=config) as app,
        AsyncTestClient(app=app) as rest_client,
        nullcontext(NamedTemporaryFile("w+b")) as temp_file,
    ):
        # Create a box
        box_id = uuid4()
        async with kafka.record_events(
            in_topic=config.file_upload_box_topic
        ) as box_recorder:
            create_box_token = utils.generate_create_file_box_token(jwk=jwk)
            headers = auth_header(create_box_token)
            box_creation_body = {"box_id": str(box_id), "storage_alias": "test"}
            response = await rest_client.post(
                "/boxes", json=box_creation_body, headers=headers
            )
            assert response.status_code == 201
        events = box_recorder.recorded_events
        assert events
        assert len(events) == 1
        assert events[0].type_ == "upserted"
        assert events[0].payload == {
            "id": str(box_id),
            "locked": False,
            "size": 0,
            "file_count": 0,
            "storage_alias": "test",
        }, "Payload was wrong for new file upload box event"

        # Make the temp test file
        file_size = 10 * 1024 * 1024  # 10 MiB
        chunk_size = 1024
        chunk = b"\0" * chunk_size
        current_size = 0
        while True:
            if current_size + chunk_size >= file_size:
                temp_file.write(chunk[: file_size - current_size])
                break
            temp_file.write(chunk)
            current_size += chunk_size
        temp_file.flush()

        # Create a FileUpload
        async with kafka.record_events(
            in_topic=config.file_upload_topic
        ) as file_recorder:
            create_file_token = utils.generate_create_file_token(
                jwk=jwk, box_id=box_id, alias="test_file"
            )
            headers = auth_header(create_file_token)
            file_creation_body = {
                "alias": "test_file",
                "checksum": "abc123",
                "size": file_size,
            }
            response = await rest_client.post(
                f"/boxes/{box_id}/uploads", json=file_creation_body, headers=headers
            )
            assert response.status_code == 201
            file_id = UUID(response.json())
        events = file_recorder.recorded_events
        assert events
        assert len(events) == 1
        assert events[0].type_ == "upserted"
        assert events[0].payload == {
            **file_creation_body,
            "id": str(file_id),
            "box_id": str(box_id),
            "completed": False,
        }, "Payload was wrong for new file upload event"

        # Get part upload URL for the file (should only require 1 part since file is under 16 MiB)
        upload_token = utils.generate_upload_file_token(
            jwk=jwk, box_id=box_id, file_id=file_id
        )
        headers = auth_header(upload_token)
        response = await rest_client.get(
            f"/boxes/{box_id}/uploads/{file_id}/parts/1", headers=headers
        )
        url = str(response.json())

        # Actually upload dummy data to S3 fixture
        temp_file.seek(0)  # Reset file pointer to beginning

        # Use httpx directly instead of the test client to bypass routing
        response = httpx.put(url, content=temp_file.read(), timeout=30)
        assert response.status_code == 200

        # File is now uploaded, so complete the upload
        async with kafka.record_events(
            in_topic=config.file_upload_topic
        ) as file_recorder:
            close_file_token = utils.generate_close_file_token(
                jwk=jwk, box_id=box_id, file_id=file_id
            )
            headers = auth_header(close_file_token)
            response = await rest_client.patch(
                f"/boxes/{box_id}/uploads/{file_id}", headers=headers
            )
        events = file_recorder.recorded_events
        assert len(events) == 1
        assert events[0].payload["completed"]

        # Let's lock the box now and verify that it is reflected in the event
        async with kafka.record_events(
            in_topic=config.file_upload_box_topic
        ) as box_recorder:
            lock_box_token = utils.generate_change_file_box_token(
                box_id=box_id, jwk=jwk
            )
            headers = auth_header(lock_box_token)
            box_update_body = {"lock": True}
            response = await rest_client.patch(
                f"/boxes/{box_id}", json=box_update_body, headers=headers
            )
            assert response.status_code == 204
        events = box_recorder.recorded_events
        assert len(events) == 1
        assert events[0].payload["locked"]

        # Now try to delete the file and verify that no event gets emitted
        async with kafka.record_events(
            in_topic=config.file_upload_topic
        ) as file_recorder:
            delete_file_token = utils.generate_delete_file_token(
                jwk=jwk, box_id=box_id, file_id=file_id
            )
            headers = auth_header(delete_file_token)
            response = await rest_client.delete(
                f"/boxes/{box_id}/uploads/{file_id}", headers=headers
            )
            assert response.status_code == 409
        assert not file_recorder.recorded_events

        # Great, we verified that the locked box prevents changes. Now unlock the box
        #  but don't check for events -- satisfied at this point that outbox is working
        box_update_body = {"lock": False}
        unlock_box_token = utils.generate_change_file_box_token(
            box_id=box_id, work_type="unlock", jwk=jwk
        )
        response = await rest_client.patch(
            f"/boxes/{box_id}",
            json=box_update_body,
            headers=auth_header(unlock_box_token),
        )
        assert response.status_code == 204

        # Delete the file finally
        response = await rest_client.delete(
            f"/boxes/{box_id}/uploads/{file_id}", headers=auth_header(delete_file_token)
        )
        assert response.status_code == 204


async def test_s3_upload_completed_but_db_not_updated(joint_fixture: JointFixture):
    """Test error handling when the S3 upload is successfully completed but a hard
    crash (simulated) causes the DB to never get updated.

    The FileUpload, S3UploadDetails, and FileUploadBox are not updated, even though
    the S3 operations were finished properly. In this case, the requester would not
    receive a meaningful error message and would have to retry the request. Upon issuing
    the request a second time, the UCS would see that the S3 upload has already been
    completed and would then update the DB documents accordingly.
    """
    controller = joint_fixture.upload_controller
    box_id = uuid4()
    async with set_correlation_id(uuid4()):
        await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
        file_id = await controller.initiate_file_upload(
            box_id=box_id, alias="test-file", checksum="abc123", size=1024
        )
    url = await controller.get_part_upload_url(file_id=file_id, part_no=1)

    # Upload the content
    response = httpx.put(url, content="a" * 1024)
    assert response.status_code == 200

    # To simulate the hiccup, we'll manually complete the upload. This will create the
    #  out-of-sync state described above.
    db = joint_fixture.mongodb.client[joint_fixture.config.db_name]
    s3_upload_details_collection = db["s3UploadDetails"]
    uploads = s3_upload_details_collection.find().to_list()
    assert len(uploads) == 1
    assert not uploads[0]["completed"]
    upload_id = uploads[0]["s3_upload_id"]

    await joint_fixture.s3.storage.complete_multipart_upload(
        bucket_id="test-inbox", object_id=str(file_id), upload_id=upload_id
    )

    # Now call the completion endpoint using the rest client
    close_token = utils.generate_close_file_token(
        box_id=box_id, file_id=file_id, jwk=joint_fixture.jwk
    )
    response = await joint_fixture.rest_client.patch(
        f"/boxes/{box_id}/uploads/{file_id}", headers=auth_header(close_token)
    )

    # Response should indicate success because the file was uploaded
    assert response.status_code == 204

    # DB should now show that everything is complete
    uploads = s3_upload_details_collection.find().to_list()
    assert len(uploads) == 1
    assert uploads[0]["completed"]
    file_upload_collection = db["fileUploads"]
    file_uploads = file_upload_collection.find(
        {"__metadata__.deleted": False}
    ).to_list()
    assert len(file_uploads) == 1
    assert file_uploads[0]["completed"]
    assert uploads[0]["_id"] == file_uploads[0]["_id"]


async def test_s3_upload_complete_fails(joint_fixture: JointFixture):
    """Test error handling when the S3 upload completion command raises a
    MultiPartUploadConfirmError.

    In this case, the requester should receive an error indicating they need to
    delete the file upload and restart the process, since no recovery is possible.
    """
    jwk = joint_fixture.jwk
    rest_client = joint_fixture.rest_client
    controller = joint_fixture.upload_controller
    box_id = uuid4()
    async with set_correlation_id(uuid4()):
        await controller.create_file_upload_box(box_id=box_id, storage_alias="test")
        file_id = await controller.initiate_file_upload(
            box_id=box_id, alias="test-file", checksum="abc123", size=1024
        )
    url = await controller.get_part_upload_url(file_id=file_id, part_no=1)

    # Upload the content
    response = httpx.put(url, content="a" * 1024)
    assert response.status_code == 200

    # To simulate the hiccup, we'll manually abort the upload. This will create the
    #  out-of-sync state described above.
    db = joint_fixture.mongodb.client[joint_fixture.config.db_name]
    s3_upload_details_collection = db["s3UploadDetails"]
    uploads = s3_upload_details_collection.find().to_list()
    assert len(uploads) == 1
    assert not uploads[0]["completed"]
    upload_id = uploads[0]["s3_upload_id"]
    await joint_fixture.s3.storage.abort_multipart_upload(
        bucket_id="test-inbox", object_id=str(file_id), upload_id=upload_id
    )

    # Make the completion request with the rest client
    close_token = utils.generate_close_file_token(
        box_id=box_id, file_id=file_id, jwk=jwk
    )
    response = await rest_client.patch(
        f"/boxes/{box_id}/uploads/{file_id}", headers=auth_header(close_token)
    )
    # Response should indicate failure and parameters
    assert response.status_code == 500
    error = response.json()
    assert error["exception_id"] == "uploadCompletionError"
    assert error["data"] == {"box_id": str(box_id), "file_id": str(file_id)}

    # Now request deletion of the file
    delete_token = utils.generate_delete_file_token(
        box_id=box_id, file_id=file_id, jwk=jwk
    )
    response = await rest_client.delete(
        f"/boxes/{box_id}/uploads/{file_id}", headers=auth_header(delete_token)
    )
    assert response.status_code == 204

    # Now retry the upload process, obtaining a new file_id
    create_token = utils.generate_create_file_token(
        box_id=box_id, alias="test-file", jwk=jwk
    )
    body = {"alias": "test-file", "checksum": "abc123", "size": 1024}
    response = await rest_client.post(
        f"/boxes/{box_id}/uploads", headers=auth_header(create_token), json=body
    )
    assert response.status_code == 201
    file_id2 = UUID(response.json())

    upload_token = utils.generate_upload_file_token(
        box_id=box_id, file_id=file_id2, jwk=jwk
    )
    response = await rest_client.get(
        f"/boxes/{box_id}/uploads/{file_id2}/parts/1", headers=auth_header(upload_token)
    )
    assert response.status_code == 200
    part_url = response.json()

    # Upload the content again
    response = httpx.put(part_url, content="a" * 1024)
    assert response.status_code == 200

    # Now complete the file
    close_token2 = utils.generate_close_file_token(
        box_id=box_id, file_id=file_id2, jwk=jwk
    )
    response = await rest_client.patch(
        f"/boxes/{box_id}/uploads/{file_id2}", headers=auth_header(close_token2)
    )
    assert response.status_code == 204

    # Check the DB to verify that docs were deleted for the old file upload and that
    #  the new file upload (for the same alias) exists instead, set to completed
    uploads = s3_upload_details_collection.find().to_list()
    assert len(uploads) == 1
    assert uploads[0]["_id"] == file_id2
    assert uploads[0]["completed"]
    file_upload_collection = db["fileUploads"]
    file_uploads = file_upload_collection.find(
        {"__metadata__.deleted": False}
    ).to_list()
    assert len(file_uploads) == 1
    assert file_uploads[0]["completed"]
    assert uploads[0]["_id"] == file_uploads[0]["_id"]
