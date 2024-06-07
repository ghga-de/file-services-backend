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

"""Generate documentation for this package using different sources."""

import json
from pathlib import Path
from string import Template

import jsonschema2md
import tomllib
from pydantic import BaseModel, Field
from stringcase import spinalcase, titlecase

from script_utils.cli import echo_failure, echo_success, run
from script_utils.utils import list_service_dirs

ROOT_DIR = Path(__file__).parent.parent.resolve()
README_TEMPLATE_PATH = ROOT_DIR / ".readme_generation" / "readme_template_service.md"


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

    name: str = Field(..., description="The full name of the package in spinal case.")
    title: str = Field(..., description="The name of the package formatted as title.")


class PackageDetails(PackageHeader, PackageName):
    """A container for details on a package used to build documentation."""

    description: str = Field(
        ..., description="A markdown-formatted description of the package."
    )
    config_description: str = Field(
        ...,
        description=(
            "A markdown-formatted list of all configuration parameters of this package."
        ),
    )
    design_description: str = Field(
        ...,
        description=(
            "A markdown-formatted description of overall architecture and design of"
            + " the package."
        ),
    )
    openapi_doc: str = Field(
        ...,
        description=(
            "A markdown-formatted description rendering or linking to an OpenAPI"
            " specification of the package."
        ),
    )


class ServiceDetails:
    """Container class for service specific paths and functionality relying on them."""

    def __init__(self, service_dir: Path):
        self.service_dir = service_dir
        self.pyproject_toml_path = service_dir / "pyproject.toml"
        self.readme_generation_dir = service_dir / ".readme_generation"
        self.description_path = self.readme_generation_dir / "description.md"
        self.design_path = self.readme_generation_dir / "design.md"
        self.config_schema_path = service_dir / "config_schema.json"
        self.openapi_yaml_path = service_dir / "openapi.yaml"
        self.readme_path = service_dir / "README.md"

    def get_package_details(self) -> PackageDetails:
        """Get details required to build documentation for the package."""

        header = self.read_toml_package_header()
        description = self.read_package_description()

        service_name = header.summary
        if "-" in service_name:
            service_name, summary = service_name.split("-", 1)
            header.summary = summary.strip()
        service_name = spinalcase(service_name.strip().replace(" ", ""))
        title = titlecase(service_name)
        name = PackageName(name=service_name, title=title)

        config_description = self.generate_config_docs()
        return PackageDetails(
            **header.model_dump(),
            **name.model_dump(),
            description=description,
            config_description=config_description,
            design_description=self.read_design_description(),
            openapi_doc=self.generate_openapi_docs(),
        )

    def read_toml_package_header(self) -> PackageHeader:
        """Read basic information about the package from the pyproject.toml"""

        with self.pyproject_toml_path.open("rb") as pyproject_toml:
            pyproject = tomllib.load(pyproject_toml)
            pyproject_project = pyproject["project"]
            return PackageHeader(
                shortname=pyproject_project["name"],
                version=pyproject_project["version"],
                summary=pyproject_project["description"],
            )

    def read_package_description(self) -> str:
        """Read the package description."""

        return self.description_path.read_text()

    def read_design_description(self) -> str:
        """Read the design description."""

        return self.design_path.read_text()

    def generate_config_docs(self) -> str:
        """Generate markdown-formatted documentation for the configration parameters
        listed in the config schema."""

        parser = jsonschema2md.Parser(
            examples_as_yaml=False,
            show_examples="all",
        )
        with open(self.config_schema_path, encoding="utf-8") as json_file:
            config_schema = json.load(json_file)

        md_lines = parser.parse_schema(config_schema)

        # ignore everything before the properties header:
        properties_index = md_lines.index("## Properties\n\n")
        md_lines = md_lines[properties_index + 1 :]

        return "\n".join(md_lines)

    def generate_openapi_docs(self) -> str:
        """Generate markdown-formatted documentation linking to or rendering an OpenAPI
        specification of the package. If no OpenAPI specification is present, return an
        empty string."""

        if not self.openapi_yaml_path.exists():
            return ""

        return (
            "## HTTP API\n"
            + "An OpenAPI specification for this service can be found"
            + f" [here]({self.openapi_yaml_path.relative_to(self.service_dir)})."
        )


def generate_single_readme(*, details: PackageDetails) -> str:
    """Generate a single markdown-formatted readme file for the package based on the
    provided details."""

    template_content = README_TEMPLATE_PATH.read_text()
    template = Template(template_content)
    return template.substitute(details.model_dump())


def main(service: str = "", check: bool = False) -> None:
    """Update the readme markdown."""

    if service:
        services_to_check = [Path(f"./services/{service}")]
    else:
        services_to_check = list_service_dirs()

    for service_dir in services_to_check:
        service_name = service_dir.name
        service_details = ServiceDetails(service_dir=service_dir)

        details = service_details.get_package_details()
        readme_content = generate_single_readme(details=details)

        if check:
            if service_details.readme_path.read_text() == readme_content:
                echo_success(f"{service_name}: README.md is up to date.")
                continue
            echo_failure(f"{service_name}: README.md is not up to date.")

        service_details.readme_path.write_text(readme_content)
        echo_success(f"{service_name}: Successfully updated README.md.")


if __name__ == "__main__":
    run(main)
