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

import io
import sys
from collections.abc import Callable
from contextlib import contextmanager, nullcontext
from functools import wraps

import typer

from script_utils import cli, utils
from update_config_docs import main as update_config
from update_hook_revs import main as update_hooks
from update_lock import main as update_lock
from update_openapi_docs import main as update_openapi
from update_pyproject import main as update_pyproject
from update_readme_monorepo import main as update_readme_root
from update_readme_services import main as update_readme_service

PREV_LINE = "\033[F\033[2K\r"  # moves up one line in the cli
REPORT_WIDTH = 35
app = typer.Typer(no_args_is_help=True, add_completion=False)


ServiceArg = typer.Argument(
    default="",
    case_sensitive=False,
    callback=utils.validate_folder_name,
)

CheckFlag = typer.Option(False, "--check")


@contextmanager
def suppress_print(service: str):
    """Temporarily suppress print statements."""
    original_stdout = sys.stdout
    temp_buffer = io.StringIO()
    sys.stdout = temp_buffer
    try:
        yield
    except Exception as e:
        sys.stdout = original_stdout
        cli.echo_warning(f"Unable to complete for '{service}'. See error below:")
        cli.echo_failure(str(e))
        exit(1)
    finally:
        sys.stdout = original_stdout


def root_specific(func: Callable) -> Callable:
    """
    A decorator that runs the decorated function and modifies output format as needed.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        check = kwargs.get("check", False)
        status = "Checking" if check else "Updating"
        func_name = func.__name__.replace("_", " ")
        line_prefix = PREV_LINE + PREV_LINE if check else PREV_LINE
        report = "Already up to date!" if check else "Done"

        print(f"( ) {status} {func_name}...")
        with suppress_print("") if not check else nullcontext():
            func(*args, **kwargs)

        cli.echo_success(f"{line_prefix}(✓) {status} {func_name}... {report}")

    return wrapper


def service_specific(func: Callable) -> Callable:
    """
    A decorator that runs the decorated function for all services if the service
    argument is an empty string, or runs it for the specified service otherwise.
    """

    @wraps(func)
    def wrapper(service: str = ServiceArg, *args, **kwargs):
        all_services = utils.list_service_dirs()
        service_count = len(all_services)
        check = kwargs.get("check", False)
        status = "Checking" if check else "Updating"
        func_name = func.__name__.replace("_", " ")
        line_prefix = PREV_LINE + PREV_LINE if check else PREV_LINE
        report = "Already up to date!" if check else "Done"

        if service == "":
            print(f"( ) {status} {func_name}...(1/{service_count})")

            for i, svc in enumerate(all_services):
                print(
                    f"{PREV_LINE}( ) {status} {func_name}...({i + 1}/{service_count}): {svc.name}",
                )
                with suppress_print(svc.name) if not check else nullcontext():
                    func(svc.name, *args, **kwargs)
                if check and i < service_count - 1:
                    print(PREV_LINE + PREV_LINE)

            cli.echo_success(
                f"{line_prefix}(✓) {status} {func_name}...({service_count}/{service_count}): {report}"
            )

        else:
            print(f"( ) {status} {func_name} for {service}...")
            func(service, *args, **kwargs)
            cli.echo_success(
                f"{PREV_LINE}{PREV_LINE}(✓) {status} {func_name} for {service}... {report}"
            )

    return wrapper


@app.command(name="lock")
@root_specific
def lock_files(check: bool = CheckFlag):
    """Update the lock files (scripts/update_lock.py + upgrade=True)."""
    update_lock(upgrade=True, check=check)


@app.command(name="pyproject")
@root_specific
def pyproject(check: bool = CheckFlag):
    """Update the root pyproject.toml file"""
    update_pyproject(check=check)


@app.command(name="config")
@service_specific
def config_docs(service: str = ServiceArg, check: bool = CheckFlag):
    """Update the config docs for one or all services (scripts/update_config_docs.py)."""
    update_config(service=service, check=check)


@app.command(name="openapi")
@service_specific
def openapi_docs(service: str = ServiceArg, check: bool = CheckFlag):
    """Update the OpenAPI docs for one or all services (scripts/update_openapi_docs.py)."""
    update_openapi(service=service, check=check)


@service_specific
def service_readme(service: str = ServiceArg, check: bool = CheckFlag):
    """Update the README for one or all services (scripts/update_readme_services.py)."""
    update_readme_service(service=service, check=check)


@app.command(name="readme-root")
@root_specific
def root_readme(check: bool = CheckFlag):
    """Update the root README only (scripts/update_readme_monorepo.py)"""
    update_readme_root(check=check)


@app.command(name="readme")
def readme(service: str = ServiceArg, check: bool = CheckFlag):
    """Update the README for a single service, or, for all of them and the root README."""
    service_readme(service=service, check=check)
    if not service:
        root_readme(check=check)


@app.command(name="hooks")
@root_specific
def precommit_hooks(check: bool = CheckFlag):
    """Update the configured pre-commit hook versions (scripts/update_hook_revs.py)."""
    update_hooks(check=check)


@app.command(name="all-for")
def update_service_specific(service: str = ServiceArg, check: bool = CheckFlag):
    """Run all *service-specific* update scripts for one or all services, in order."""
    print(f"Running all scripts for {service if service else "all services"}.")
    config_docs(service=service, check=check)
    openapi_docs(service=service, check=check)
    service_readme(service=service, check=check)


@app.command(name="all")
def update_all(check: bool = CheckFlag):
    """Run all update scripts for everything in order.

    Scripts are run in order to account for downstream changes, such as config -> readme.
    Service-specific scripts are run for all services.
    """
    lock_files(check=check)
    precommit_hooks(check=check)
    pyproject(check=check)
    config_docs(service="", check=check)
    openapi_docs(service="", check=check)
    service_readme(service="", check=check)
    root_readme(check=check)


if __name__ == "__main__":
    app()
