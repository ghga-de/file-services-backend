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
"""Session-scoped fixture setup"""

import pytest
from hexkit.correlation import correlation_id_var, new_correlation_id
from hexkit.providers.akafka.testutils import (  # noqa: F401
    kafka_container_fixture,
    kafka_fixture,
)
from hexkit.providers.mongodb.testutils import (  # noqa: F401
    mongodb_container_fixture,
    mongodb_fixture,
)
from hexkit.providers.s3.testutils import (  # noqa: F401
    s3_container_fixture,
    s3_fixture,
    tmp_file,  # function-scoped
)

# These are function-scoped but importing them here saves imports in test
# modules and makes it so the noqa: F811 is not required
from tests_dcs.fixtures.joint import (  # noqa: F401
    cleanup_fixture,
    joint_fixture,
    populated_fixture,
)


@pytest.fixture(autouse=True)
def use_correlation_id():
    """Provides a new correlation ID for each test case."""
    correlation_id = new_correlation_id()
    token = correlation_id_var.set(correlation_id)
    yield
    correlation_id_var.reset(token)
