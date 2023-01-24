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

"""A collection of http responses."""


from fastapi.responses import JSONResponse


class HttpObjectNotInOutboxResponse(JSONResponse):

    """
    Returned, when a file has not been staged to the outbox yet.
    """

    response_id = "objectNotInOutbox"

    def __init__(
        self,
        *,
        status_code: int = 202,
        retry_after: int = 300,
    ):

        headers = {"Retry-After": str(retry_after)}

        """Construct message and init the response."""
        super().__init__(content=None, status_code=status_code, headers=headers)
