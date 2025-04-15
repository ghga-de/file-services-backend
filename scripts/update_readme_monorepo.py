#!/usr/bin/env python3

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

"""Generate documentation for this package using different sources."""

import subprocess  # nosec
import sys
import tomllib
from pathlib import Path
from string import Template

from casefy import kebabcase, titlecase
from pydantic import BaseModel, Field

from script_utils.cli import echo_failure, echo_success, run
from script_utils.utils import list_service_dirs

ROOT_DIR = Path(__file__).parent.parent.resolve()
PYPROJECT_TOML_PATH = ROOT_DIR / "pyproject.toml"
README_GENERATION_DIR = ROOT_DIR / ".readme_generation"
DESCRIPTION_PATH = README_GENERATION_DIR / "description.md"
CONFIGURATION_PATH = README_GENERATION_DIR / "configuration.md"
README_TEMPLATE_PATH = README_GENERATION_DIR / "readme_template_monorepo.md"
README_PATH = ROOT_DIR / "README.md"
SERVICE_ROOT = ROOT_DIR / "services"


class PackageHeader(BaseModel):
    """A basic summary of a package."""

    shortname: str = Field(
        ...,
        description=(
            "The abbreviation of the package name. Is identical to the package name."
        ),
    )
    version: str = Field(..., description="The version of the package.")
    summary: str = Field(
        ..., description="A short 1 or 2 sentence summary of the package."
    )


class PackageName(BaseModel):
    """The name of a package and it's different representations."""

    repo_name: str = Field(..., description="The name of the repo")
    name: str = Field(..., description="The full name of the package in spinal case.")
    title: str = Field(..., description="The name of the package formatted as title.")


class PackageDetails(PackageHeader, PackageName):
    """A container for details on a package used to build documentation."""

    description: str = Field(
        ..., description="A markdown-formatted description of the package."
    )
    configuration: str = Field(
        ...,
        description="A markdown-formatted description of configuration options relevant for deployment.",
    )
    service_readmes: str = Field(..., description="")


def read_toml_package_header() -> PackageHeader:
    """Read basic information about the package from the pyproject.toml"""

    with PYPROJECT_TOML_PATH.open("rb") as pyproject_toml:
        pyproject = tomllib.load(pyproject_toml)
        pyproject_project = pyproject["project"]
        return PackageHeader(
            shortname=pyproject_project["name"],
            version=pyproject_project["version"],
            summary=pyproject_project["description"],
        )


def read_service_description(service_dir: Path) -> str:
    """Read service name from a service pyproject.toml"""

    service_pyproject_toml_path = service_dir / "pyproject.toml"
    with service_pyproject_toml_path.open("rb") as pyproject_toml:
        pyproject = tomllib.load(pyproject_toml)
        pyproject_project = pyproject["project"]
        return pyproject_project["description"]


def read_package_name() -> PackageName:
    """Infer the package name from the name of the git origin."""

    with subprocess.Popen(
        args="basename -s .git `git config --get remote.origin.url`",
        cwd=ROOT_DIR,
        stdout=subprocess.PIPE,
        shell=True,
    ) as process:
        stdout, _ = process.communicate()

    if not stdout:
        raise RuntimeError("The name of the git origin could not be resolved.")
    git_origin_name = stdout.decode("utf-8").strip()

    repo_name = kebabcase(git_origin_name)
    name = (
        "my-microservice"
        if repo_name == "microservice-repository-template"
        else repo_name
    )
    title = titlecase(name)

    return PackageName(repo_name=repo_name, name=name, title=title)


def read_package_description() -> str:
    """Read the package description."""

    return DESCRIPTION_PATH.read_text()


def read_package_configuration() -> str:
    """Read the package description."""

    return CONFIGURATION_PATH.read_text()


def get_service_links() -> str:
    """Get links to all service readmes."""

    service_readme_links = []

    for service_dir in sorted(list_service_dirs()):
        service_description = read_service_description(service_dir)
        readme_link = service_dir.relative_to(ROOT_DIR)
        if "-" in service_description:
            service_description = service_description.split("-")[0].strip()
        service_readme_links.append(f"[{service_description}]({readme_link})")

    return "  \n".join(service_readme_links)


def get_package_details() -> PackageDetails:
    """Get details required to build documentation for the package."""

    header = read_toml_package_header()
    name = read_package_name()
    description = read_package_description()
    configuration = read_package_configuration()
    return PackageDetails(
        **header.model_dump(),
        **name.model_dump(),
        description=description,
        service_readmes=get_service_links(),
        configuration=configuration,
    )


def generate_single_readme(*, details: PackageDetails) -> str:
    """Generate a single markdown-formatted readme file for the package based on the
    provided details."""

    template_content = README_TEMPLATE_PATH.read_text()
    template = Template(template_content)
    return template.substitute(details.model_dump())


def main(check: bool = False) -> None:
    """Update the readme markdown."""

    details = get_package_details()
    readme_content = generate_single_readme(details=details)

    if check:
        if README_PATH.read_text() != readme_content:
            echo_failure("README.md is not up to date.")
            sys.exit(1)
        echo_success("README.md is up to date.")
        return

    README_PATH.write_text(readme_content)
    echo_success("Successfully updated README.md.")


if __name__ == "__main__":
    run(main)
