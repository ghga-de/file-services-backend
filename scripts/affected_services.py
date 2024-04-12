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

"""Determine which services are impacted by current changes."""

import subprocess
from pathlib import Path

import typer

from script_utils.utils import list_service_dirs

HERE = Path(__file__).parent.resolve()
REPO_ROOT_DIR = HERE.parent
SERVICES_DIR = REPO_ROOT_DIR / "services"


def must_run_all(non_service_changes: list[str]) -> bool:
    """Determine if any changes outside the service dir require running all CI checks."""
    for file in non_service_changes:
        if file.startswith(
            (
                ".github/",
                ".pyproject_generation/",
                ".readme_generation/",
                ".template/",
                ".lock/",
                "scripts/",
                "pyproject.toml",
            )
        ) or Path(file).name in (
            ".pre-commit-config.yaml",
            "license_header.txt",
            "template-Dockerfile",
        ):
            return True
    return False


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


def files_in_diff(full: bool, target: str) -> list[str]:
    """List files in diff."""
    # Command to list names of changed files
    change_range = f"{target}...HEAD" if full else "HEAD HEAD~1"
    command = f"git diff --name-only {change_range}"

    # Execute the command and capture the output
    result = subprocess.run(
        command, shell=True, text=True, capture_output=True, check=True
    )

    # The stdout attribute contains the command's output
    changed_files = result.stdout.strip().split("\n")

    # Print each changed file
    return changed_files


def on_main_branch() -> bool:
    """Pointless to execute on main branch, so check for current branch name."""
    git_check = "git branch --show-current"
    output = subprocess.run(
        git_check, shell=True, text=True, capture_output=True, check=True
    )
    branch_name = output.stdout.strip()
    return branch_name == "main"


def main(
    *,
    full: bool = typer.Option(
        False,
        help="If set, runs for all changes in branch. Otherwise runs for current commit.",
    ),
    target: str = typer.Option(
        "main",
        help='Which branch to compare against. Defaults to "main". Has no effect if full=False.',
    ),
):
    """Determine if changes require running CI checks for all or a subset of services.

    Output is a comma-delimited string of affected services.
    """
    if on_main_branch():
        return

    files = files_in_diff(full=full, target=target)

    # In practice, changes should affect either one or all services, but that is not
    # assumed to always hold true.
    modified_services = get_modified_services(files)
    non_service_changes = get_top_level_changes(files)

    affected_services = (
        [path.name for path in list_service_dirs()]
        if must_run_all(non_service_changes)
        else modified_services
    )
    print(",".join(affected_services))


if __name__ == "__main__":
    typer.run(main)
