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

"""Contains module-scoped fixtures"""

from hexkit.providers.akafka.testutils import (
    get_clean_kafka_fixture,
    kafka_container_fixture,  # noqa: F401
)
from hexkit.providers.mongodb.testutils import (
    get_clean_mongodb_fixture,
    mongodb_container_fixture,  # noqa: F401
)
from hexkit.providers.s3.testutils import (  # noqa: F401
    get_clean_s3_fixture,
    s3_container_fixture,
)

from tests_ifrs.fixtures.joint import JointFixture, get_joint_fixture  # noqa: F401

mongodb = get_clean_mongodb_fixture("session")
kafka = get_clean_kafka_fixture("session")
s3 = get_clean_s3_fixture("session")
second_s3 = get_clean_s3_fixture("session", name="second_s3")
joint_fixture = get_joint_fixture("session")
