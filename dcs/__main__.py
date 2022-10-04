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


"""
Entrypoint for the package.
"""

from wsgiref.simple_server import make_server

from .api.main import get_app
from .config import CONFIG, Config

app = get_app()


def run(config: Config = CONFIG) -> None:
    """
    Starts backend server
    """
    server = make_server(config.host, config.port, app)
    server.serve_forever()


if __name__ == "__main__":
    run()
