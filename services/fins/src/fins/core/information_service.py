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


import ghga_event_schemas.pydantic_ as event_schemas

from fins.ports.inbound.information_service import InformationServicePort


class InformationService(InformationServicePort):
    """TODO"""

    def __init__(self): ...

    def deletion_requested(self, file_id: str):
        """TODO"""

    def register_information(
        self, file_information: event_schemas.FileInternallyRegistered
    ):
        """TODO"""
