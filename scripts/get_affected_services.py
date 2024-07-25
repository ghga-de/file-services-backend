#!/usr/bin/env python3

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

"""Determine which services are impacted by current changes."""

import json
from pathlib import Path

import typer

from script_utils.utils import list_service_dirs

HERE = Path(__file__).parent.resolve()
REPO_ROOT_DIR = HERE.parent
SERVICES_DIR = REPO_ROOT_DIR / "services"


def must_run_all(non_service_changes: list[str]) -> bool:
    """Determine if any changes outside the service dir require running CI for all services."""

    return any(
        file
        for file in non_service_changes
        if (
            file
            in (
                ".github/workflows/ci_workflow_dispatch.yaml",
                ".github/workflows/docker_on_release.yaml",
            )  # no need to include the other workflows as they are basically just wrappers
            or file.startswith(
                (
                    "lock/",
                    "scripts/",
                )
            )
            or Path(file).name == "Dockerfile"
        )
    )


def get_modified_services(files: list[str]) -> list[str]:
    """Turn a list of files from the services/ dir into a list of affected services."""
    services: set[str] = set()
    for file in files:
        if file.startswith("services/") and (service := file.split("/")[1]):
            services.add(service)
    return list(services)


def get_top_level_changes(files: list[str]) -> list[str]:
    """From full diff, get top-level file changes."""
    return [file for file in files if not file.startswith("services/")]


def main(changed_files: list[str]):
    """Determine if changes require running CI checks for all or a subset of services.

    Output is a comma-delimited string of affected services.
    """

    # In practice, changes should affect either one or all services, but that is not
    # assumed to always hold true.
    modified_services = get_modified_services(changed_files)
    non_service_changes = get_top_level_changes(changed_files)

    affected_services = (
        [path.name for path in list_service_dirs()]
        if must_run_all(non_service_changes)
        else modified_services
    )
    print(json.dumps(affected_services))


if __name__ == "__main__":
    typer.run(main)
