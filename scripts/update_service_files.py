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
"""A CLI tool to aid in running scripts."""

from functools import wraps
from typing import Callable

import typer

from script_utils import utils
from update_config_docs import main as update_config
from update_hook_revs import main as update_hooks
from update_openapi_docs import main as update_openapi
from update_readme_services import main as update_readmes

app = typer.Typer(no_args_is_help=True, add_completion=False)


ServiceArg = typer.Argument(
    default="",
    case_sensitive=False,
    callback=utils.validate_folder_name,
)

CheckFlag = typer.Option(False, "--check")


def run_for_service_or_all(func: Callable) -> Callable:
    """
    A decorator that runs the decorated function for all services if the service
    argument is an empty string, or runs it for the specified service otherwise.
    """

    @wraps(func)
    def wrapper(service: str = ServiceArg, *args, **kwargs):
        all_services = utils.list_service_dirs()

        if service == "":
            for svc in all_services:
                func(svc.name, *args, **kwargs)
        else:
            func(service, *args, **kwargs)

    return wrapper


@app.command(name="config")
@run_for_service_or_all
def config(service: str = ServiceArg, check: bool = CheckFlag):
    """Run scripts/update_config_docs.py for one or all services."""
    update_config(service=service, check=check)


@app.command(name="openapi")
@run_for_service_or_all
def openapi(service: str = ServiceArg, check: bool = CheckFlag):
    """Run scripts/update_openapi_docs.py for one or all services."""
    update_openapi(service=service, check=check)


@app.command(name="one")
@run_for_service_or_all
def update_one_service(service: str = ServiceArg, check: bool = CheckFlag):
    """Run all service-specific update scripts for one or all services.

    This will update the config, pyproject, and openapi.
    """
    print(f"Updating all for {service}")
    update_config(service=service, check=check)
    update_openapi(service=service, check=check)


@app.command(name="hooks")
def hooks(check: bool = CheckFlag):
    """Run scripts/update_hook_revs.py."""
    update_hooks(check=check)


@app.command(name="all")
def update_everything(check: bool = CheckFlag):
    """Run all update scripts. Service-specific scripts are run for all services."""
    hooks(check=check)
    update_one_service(service="", check=check)
    update_readmes()


if __name__ == "__main__":
    app()
