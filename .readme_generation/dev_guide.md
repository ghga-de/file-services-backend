# Monorepo Development

## Overview

This document aims to provide a thorough understanding of this monorepo setup. It covers
file layout, dependency management, tooling, common processes, and more.
Because there may be multiple monorepos in use by GHGA, the information is agnostic 
towards the content therein.

## Monorepo Structure

The monorepo houses multiple microservice projects while maintaining isolation between
each service. Services are located in the [`services`](/services) folder in the root directory.
| Root-Level Directory | Description |
|-----------|-------------|
| [`/.github`](/.github)  | Workflows to be executed on GitHub |
| [`/.readme_generation`](/.readme_generation) | Template files required to build the root-level README |
| [`/lock`](/lock) | Dependency specifications and resulting lock files |
| [`/scripts`](/scripts) | Custom scripts for common tasks such as updating lock files or config docs |
| [`/services`](/services) | Application code for each of the microservices in the monorepo |


### Dependency Management

Dependency management consists of defining 3rd party python package requirements and
pinning explicit versions of those requirements in what's known as a _lock file_. When
dependencies are installed for the project, they are installed using that lock file,
which is stored as [`requirements.txt`](/lock/requirements.txt) in `/lock`.
Dependencies are shared across all services in the monorepo, so it is not possible, for 
example, to use hexkit v2 in one service and hexkit v3 in another. 
There is a templating system in place that allows for cleaner control over dependencies.
Here's how it works:
1. In `/lock`, there are several `requirements-*` files:
   - `requirements-dev-template.in`: Contains *uncapped* (no upper bound) dependencies
     that are controlled by the Microservice Template Repository and used for development
     in all GHGA microservice projects (including monorepos).
   - `requirements-dev.in`: Contains any other dependencies required for development of
     this repository specifically.
   - `requirements-dev.txt`: Lock file containing application *and* development dependencies.
   - `requirements.txt`: Lock file containing *only* application dependencies.

2. `.pyproject_generation/pyproject_custom.toml` is used to define core application
dependencies, without which the service cannot run. When changes are made,
`scripts/update_pyproject.py` must be run to update the `pyproject.toml` file at the
root level.

3. `scripts/update_lock.py --upgrade` is used to combine information from `requirements-dev-template.in`,
`requirements-dev.in`, and `pyproject.toml` to build the two lock files listed above.


### Monorepo Configuration

Outside of the services, there are three primary points of configuration: `pyproject.toml`, 
`.pre-commit-config.yaml` and the contents of `.devcontainer`.

The root-level pyproject.toml file should not be updated directly. Instead the files
in the `.pyproject_generation` folder should be adjusted. Tooling configuration for ruff,
mypy, pytest, etc. are contained in `pyproject_template.toml`. When making changes,
remember to run the update script to ensure changes are reflected in the actual
pyproject.toml file.

Pre-commit is configured through the .pre-commit-config.yaml found in the root directory.
There are standard pre-commit checks as well as 3rd party checks from ruff and mypy. 
The locally-sourced check at the top of the file is used to ensure that ruff, mypy, and
any other checks are kept up to date with their package versions listed in the lock file.

### Tooling Scripts

There are an assortment of scripts developed in-house to aid in common development work.
These scripts are all contained in `/scripts`. The titles should be self-explanatory.

There is a keystroke-friendly command copied over with the devcontainer called
`update_service_files` that can be used to execute the scripts on one or all services.
- E.g.: `update_service_files openapi ifrs` will update the openapi docs for just the ifrs.
- Run `update_service_files` alone to see command help.

### Code Quality Tools

We previously used Black, flake8, iSort, and other tools to enforce rigid standards for
code quality, but these have been replaced with `ruff` for both linting and formatting. 
Mypy supplies type-checking help. These tools are configured via the pyproject.toml file
and run with pre-commit (as well as manually).

### CI/CD

The CI/CD pipeline is largely carried out by GitHub actions and workflows. The monorepo
repository is concerned only with the workflows defined in `.github/workflows/`.
Here, each of the files represents a workflow that is executed on GitHub upon a configured
point, such as when new changes are pushed, or when a PR is opened. 

The defined workflows carry out static code analysis checks, execute tests, and push
new images to Docker Hub. To ensure that workflows are only triggered when necessary, the
[`get_affected_services`](/.github/workflows/get_affected_services.yaml) workflow is used
to examine git history and only run workflows for services affected by the current branch.

### Docker

TBD

## Service Structure

Services consist of the following high-level parts:
- `.readme_generation/`: Template components required to build the service-specific README.
- `scripts/app_openapi.py`: Required only if the service uses FastAPI for a REST API. 
- `src`: Application code stored here in a subdirectory labeled with the service abbreviation.
- `tests_<service_name>`: E.g. `tests_pcs`. All service-specific tests are stored here.
- Config Files: `config_schema.json`, `dev_config.yaml`, `example_config.yaml`
- `openapi.yaml`: The OpenAPI specification (only required if using FastAPI).
- `pyproject.toml`: The service's metadata, used to install the package.
- `README.md`: Describes the service's purpose, configuration, and design.

### Service Configuration

There is one config file for each directory under `services/` called `dev_config.yaml`.
The config is defined in application code using Pydantic, and the `scripts/update_config_docs.py`
script uses the code (a class called `Config`) and the dev_config.yaml file to compile
both `config_schema.json` and `example_config.yaml`. 

The dev_config.yaml for each service is loaded into the development environment when the
devcontainer is activated, but *only* if it is listed in `.devcontainer/docker-compose.yml`
under the `environment` section as <service_name>_CONFIG_YAML. 
- E.g.: `IRS_CONFIG_YAML: /workspace/services/irs/dev_config.yaml`

### Testing

Testing is performed with `pytest`, which is configured in the pyproject.toml at the repo
root. At the service level, tests are stored in the folder named `tests_<service_name>`, 
e.g. `tests_ifrs` for the ifrs project or `tests_pcs` for the pcs project. The tests
folder lives at the root of the service-specific directory, i.e. 
`services/<service_name>/tests_<service_name>`. This naming convention prevents namespace
confusion, allowing for cleanly divided service-specific test directories. Otherwise,
import errors occur and pytest has difficulty collecting tests.

Tests can be run for all services with the command `pytest`. For a specific service only,
add the service directory: `pytest services/ifrs`. 

## Versioning

Service package versions are maintained in the service-specific pyproject.toml files.
The monorepo version number is updated via pyproject_custom.toml in .pyproject_generation.
For a given version change, only the highest-impact changes need to be considered. For 
example, imagine two PRs are merged before a version number is bumped and a release made.
PR #1 is a low-impact change that fixes a small bug. No API changes are performed.
PR #2, however, modifies several configuration parameters and adds a REST API endpoint.
PR #1 is a patch-number change, and PR #2 is a major-number change. Therefore, when
updating the monorepo version number for a release with these two changes, the monorepo
major version number should be incremented, not the patch number.

## Development Conventions

Development in a monorepo setup requires adherence to conventions to minimize developer
blocking. All changes that span >1 service should be executed in a dedicated PR. This
includes lock file/dependency updates, widespread refactoring, library changes, etc.
Normally, branches and the resulting PR should be isolated to one service. This ensures
that developers do not inadvertently create merge conflicts. Coordinate monorepo-spanning
changes with other developers.
