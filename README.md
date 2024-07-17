[![tests](https://github.com/ghga-de/file-services-backend/actions/workflows/tests.yaml/badge.svg)](https://github.com/ghga-de/file-services-backend/actions/workflows/tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/file-services-backend/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/file-services-backend?branch=main)

# File Services Backend

File Services Backend - monorepo housing file services

## Description

This is a monorepo containing all GHGA file backend microservices.


## Services:

[Download Controller Service](services/dcs)  
[Encryption Key Store Service](services/ekss)  
[File Ingest Service](services/fis)  
[Internal File Registry Service](services/ifrs)  
[Interrogation Room Service](services/irs)  
[Purge Controller Service](services/pcs)  
[Upload Controller Service](services/ucs)

## Development:

For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of VS Code
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as VS Code with its "Remote - Containers"
extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in VS Code and run the command
`Remote-Containers: Reopen in Container` from the VS Code "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the services (databases, etc.)
- all relevant VS Code extensions pre-installed
- pre-configured linting and auto-formatting
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, a convenience command `dev_install` is available.
It installs the services with all development dependencies and installs pre-commit.

The installation is performed automatically when you build the devcontainer. However,
if you update dependencies in the [`./pyproject.toml`](./pyproject.toml) or the
[`./requirements-dev.txt`](./requirements-dev.txt), please run it again.

For more information on development with this monorepo, please see the 
[Developer Guide](./.readme_generation/dev_guide.md).

## License

This repository is free to use and modify according to the
[Apache 2.0 License](./LICENSE).

## README Generation

This README file is auto-generated, please see [`readme_generation.md`](./readme_generation.md)
for details.
