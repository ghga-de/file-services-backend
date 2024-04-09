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
#

"""A script to update the pyproject.toml."""

import os
import sys
from contextlib import contextmanager
from pathlib import Path

import tomli
import tomli_w

from script_utils import cli

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()
SERVICES_DIR = REPO_ROOT_DIR / "services"

PYPROJECT_GENERATION_DIR = REPO_ROOT_DIR / ".pyproject_generation"
SERVICE_TEMPLATE_PATH = PYPROJECT_GENERATION_DIR / "service_template.toml"
PYPROJECT_TEMPLATE_PATH = PYPROJECT_GENERATION_DIR / "pyproject_template.toml"
pyproject_custom_path = PYPROJECT_GENERATION_DIR / "pyproject_custom.toml"
pyproject_toml = REPO_ROOT_DIR / "pyproject.toml"


@contextmanager
def set_service_specific_vars(service: str):
    """Adjust global vars for service."""
    global pyproject_custom_path, pyproject_toml

    # verify that the folder exists
    service_dir = SERVICES_DIR / service
    if not service_dir.exists():
        cli.echo_failure(f"{service_dir} does not exist")
        exit(1)

    # set the vars
    service_dev_dir = service_dir / ".dev"
    pyproject_custom_path = service_dev_dir / "pyproject_custom.toml"
    pyproject_toml = service_dir / "pyproject.toml"

    yield

    # reset the vars
    pyproject_custom_path = PYPROJECT_GENERATION_DIR / "pyproject_custom.toml"
    pyproject_toml = REPO_ROOT_DIR / "pyproject.toml"


def read_template_pyproject() -> dict[str, object]:
    """Read the pyproject_template.toml."""
    with open(PYPROJECT_TEMPLATE_PATH, "rb") as file:
        return tomli.load(file)


def read_custom_pyproject() -> dict[str, object]:
    """Read the pyproject_custom.toml."""
    with open(pyproject_custom_path, "rb") as file:
        return tomli.load(file)


def read_supplemental_pyproject() -> dict[str, object]:
    """Read the service_template.toml."""
    with open(SERVICE_TEMPLATE_PATH, "rb") as file:
        return tomli.load(file)


def read_current_pyproject() -> dict[str, object]:
    """Read the current pyproject.toml."""
    with open(pyproject_toml, "rb") as file:
        return tomli.load(file)


def write_pyproject(pyproject: dict[str, object]) -> None:
    """Write the given pyproject dict into the pyproject.toml."""
    with open(pyproject_toml, "wb") as file:
        tomli_w.dump(pyproject, file)


def merge_fields(*, source: dict[str, object], dest: dict[str, object]):
    """Merge fields existing in both custom and template pyproject definitions.

    If a given field is a dictionary, merge or assign depending on if it's found in dest.
    If the field is anything else either assign the value or exit with a message if a
    conflict exists.
    """
    for field, value in source.items():
        if isinstance(value, dict):
            if field in dest:
                merge_fields(source=source[field], dest=dest[field])  # type: ignore
            else:
                dest[field] = value
        else:
            if field in dest and value != dest[field]:
                cli.echo_failure(f"Conflicting values for '{field}'")
                exit(1)
            elif field not in dest:
                dest[field] = value


def merge_pyprojects(inputs: list[dict[str, object]]) -> dict[str, object]:
    """Compile a pyproject dict from the provided input dicts."""
    pyproject = inputs[0]

    for input in inputs[1:]:
        for field, value in input.items():
            if field not in pyproject:
                pyproject[field] = value
            else:
                merge_fields(source=value, dest=pyproject[field])  # type: ignore

    return pyproject


def process_pyproject(*, root: bool, check: bool):
    """Update the pyproject.toml or checks for updates if the check flag is specified."""

    template_pyproject = read_template_pyproject()
    custom_pyproject = read_custom_pyproject()
    sources = [custom_pyproject, template_pyproject]
    if not root:
        sources.append(read_supplemental_pyproject())
        template_pyproject.pop("tool", "")
    merged_pyproject = merge_pyprojects(sources)

    if check:
        current_pyproject = read_current_pyproject()

        if current_pyproject != merged_pyproject:
            cli.echo_failure("The pyproject.toml is not up to date.")
            sys.exit(1)

        cli.echo_success("The pyproject.toml is up to date.")
        return

    write_pyproject(merged_pyproject)


def main(*, check: bool = False):
    """Update the pyproject.toml or checks for updates if the check flag is specified."""
    process_pyproject(root=True, check=check)

    for service in os.listdir(SERVICES_DIR):
        with set_service_specific_vars(service=service):
            process_pyproject(root=False, check=check)
    cli.echo_success("Successfully updated all pyproject.toml files.")


if __name__ == "__main__":
    cli.run(main)
