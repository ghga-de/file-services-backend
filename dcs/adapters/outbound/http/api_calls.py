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

"""HTTP calls to other service APIs happen here"""

import base64

import requests

from dcs.adapters.outbound.http import exceptions
from dcs.adapters.outbound.http.exception_translation import ResponseExceptionTranslator


def call_eks_api(*, secret_id: bytes, receiver_public_key: str, api_url: str) -> bytes:
    """Calls EKS to get an envelope for an encrypted file, using the receivers
    public key as well as the id of the file secret."""
    request_body = {"secret_id": secret_id, "client_pk": receiver_public_key}
    try:
        response = requests.post(url=api_url, json=request_body, timeout=60)
    except requests.exceptions.RequestException as request_error:
        raise exceptions.RequestFailedError(url=api_url) from request_error

    status_code = response.status_code
    # implement httpyexpect error conversion
    if status_code != 200:
        spec: dict[int, object] = {
            404: {
                "secretNotFoundError": exceptions.SecretNotFoundError(
                    secret_id=secret_id
                )
            },
        }
        ResponseExceptionTranslator(spec=spec).handle(response=response)
        raise exceptions.BadResponseCodeError(url=api_url, response_code=status_code)

    body = response.json()
    content = base64.b64decode(body["content"])

    return content
