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
"""Mock EKSS endpoints"""

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, Response
from httpyexpect.server import HttpCustomExceptionBase


class HttpSecretNotFoundError(HttpCustomExceptionBase):
    """Thrown when no secret with the given id could be found"""

    exception_id = "secretNotFoundError"

    def __init__(self, *, status_code: int = 404):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description="The secret for the given id was not found.",
            data={},
        )


class HttpException(Exception):
    """Testing stand in for httpyexpect HttpException without content validation"""

    def __init__(
        self, *, status_code: int, exception_id: str, description: str, data: dict
    ):
        self.status_code = status_code
        self.exception_id = exception_id
        self.description = description
        self.data = data
        super().__init__(description)


app = FastAPI()


@app.exception_handler(HttpException)
async def httpy_exception_handler(request: Request, exc: HttpException):
    """Transform HttpException data into a proper response object"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "exception_id": exc.exception_id,
            "description": exc.description,
            "data": exc.data,
        },
    )


@app.get("/ready", summary="readiness_probe")
async def ready():
    """
    Readiness endpoint for container
    """
    return Response(None, status_code=status.HTTP_204_NO_CONTENT)


@app.get(
    "/secrets/{secret_id}/envelopes/{receiver_public_key}", summary="ekss_api_call_mock"
)
async def ekss_mock(secret_id: str, receiver_public_key: str):
    """
    Mock for the drs3 /objects/{file_id} call
    """
    valid_secret = "some-secret"

    if secret_id != valid_secret:
        raise HttpSecretNotFoundError()

    envelope = (
        "pfAcB7o2lz0075VTpb6b5PCdfWnPofyZ62RYxQ6gZflUoCuwSt//R2N6QCWTnn7wV/oU8syQBCgB/1KTqz77v"
        + "8jBF73IyszJzVezDokPe8AJIEFG18luo/ZRI9mDSEI/GFy2EtNdflqW+CBSgUEWiQjkRAwS3V+dVeFsVQ=="
    )

    return {"content": envelope}
