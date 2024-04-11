#!/usr/bin/env python3

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

"""Keep deployment Dockerfiles in sync"""

import sys
from pathlib import Path
from string import Template

from script_utils import cli

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()
TEMPLATE_PATH = REPO_ROOT_DIR / "template-Dockerfile"
SERVICES_DIR = REPO_ROOT_DIR / "services"


def read_file(path: Path):
    """Read the contents of a file."""
    with open(path) as file:
        return file.read()


def read_dockerfile_template():
    """Read the template Dockerfile."""
    raw_template = read_file(TEMPLATE_PATH)
    return Template(raw_template)


def main(*, service: str, check: bool = False):
    """Update the deployment Dockerfiles for each service to keep them consistent."""
    template = read_dockerfile_template()

    service_path = SERVICES_DIR / service

    expected = template.substitute({"entrypoint": service_path.name})
    if check:
        current = read_file(service_path / "Dockerfile")
        if current.strip() != expected.strip():  # don't fail only for trailing ws
            cli.echo_failure(f"Dockerfile for {service} is outdated.")
            sys.exit(1)
        else:
            return
    else:
        with open(service_path / "Dockerfile", "w") as file:
            file.write(expected)
        cli.echo_success(f"Successfully updated Dockerfile for {service}.")


if __name__ == "__main__":
    cli.run(main)
