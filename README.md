
[![tests](https://github.com/ghga-de/encryption-key-store-service/actions/workflows/unit_and_int_tests.yaml/badge.svg)](https://github.com/ghga-de/encryption-key-store-service/actions/workflows/unit_and_int_tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/encryption-key-store-service/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/encryption-key-store-service?branch=main)

# Encryption Key Store Service

Encryption Key Store Sevice - providing crypt4gh file secret extraction, storage and envelope generation

## Description

<!-- Please provide a short overview of the features of this service.-->

This service implements an interface to extract file encryption secrects from a
[GA4GH Crypt4GH](https://www.ga4gh.org/news/crypt4gh-a-secure-method-for-sharing-human-genetic-data/)
encrypted file into a HashiCorp Vault and produce user-specific file envelopes
containing these secrets.


### API endpoints:

#### `POST /secrets`:

This endpoint takes in the first part of a crypt4gh encrypted file that contains the
file envelope and a client public key.
It decrypts the envelope, using the clients public and GHGA's private key to obtain
the original encryption secret.
Subsequently, a new random secret that can be used for re-encryption and is created
and stored in the vault.
The original secret is *not* saved in the vault.

This endpoint returns the extracted secret, the newly generated secret, the envelope offset
(length of the envelope) and the secret id which can be used to retrieve the new secret from the vault.


#### `GET /secrets/{secret_id}/envelopes/{client_pk}`:

This endpoint takes a secret_id and a client public key.
It retrieves the corresponding secret from the vault and encrypts it with GHGAs
private key and the clients public key to create a crypt4gh file envelope.

This enpoint returns the envelope.


#### `DELETE /secrets/{secret_id}`:

This endpoint takes a secret_id.
It deletes the corresponding secret from the Vault.
This enpoint returns a 204 Response, if the deletion was successfull
or a 404 response, if the secret_id did not exist.


## Installation
We recommend using the provided Docker container.

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/encryption-key-store-service):
```bash
docker pull ghga/encryption-key-store-service:0.3.5
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/encryption-key-store-service:0.3.5 .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/encryption-key-store-service:0.3.5 --help
```

If you prefer not to use containers, you may install the service from source:
```bash
# Execute in the repo's root dir:
pip install .

# To run the service:
ekss --help
```

## Configuration
### Parameters

The service requires the following configuration parameters:
- **`debug_vault`** *(boolean)*: If true, runs vault connections over http instead of https. Default: `False`.

- **`vault_host`** *(string)*: URL of the vault instance to connect to without port number.

- **`vault_port`** *(integer)*: Port number of the vault instance to connect to.

- **`vault_role_id`** *(string)*: Vault role ID to access a specific prefix.

- **`vault_secret_id`** *(string)*: Vault secret ID to access a specific prefix.

- **`host`** *(string)*: IP of the host. Default: `127.0.0.1`.

- **`port`** *(integer)*: Port to expose the server on the specified host. Default: `8080`.

- **`log_level`** *(string)*: Controls the verbosity of the log. Must be one of: `['critical', 'error', 'warning', 'info', 'debug', 'trace']`. Default: `info`.

- **`auto_reload`** *(boolean)*: A development feature. Set to `True` to automatically reload the server upon code changes. Default: `False`.

- **`workers`** *(integer)*: Number of workers processes to run. Default: `1`.

- **`api_root_path`** *(string)*: Root path at which the API is reachable. This is relative to the specified host and port. Default: `/`.

- **`openapi_url`** *(string)*: Path to get the openapi specification in JSON format. This is relative to the specified host and port. Default: `/openapi.json`.

- **`docs_url`** *(string)*: Path to host the swagger documentation. This is relative to the specified host and port. Default: `/docs`.

- **`cors_allowed_origins`** *(array)*: A list of origins that should be permitted to make cross-origin requests. By default, cross-origin requests are not allowed. You can use ['*'] to allow any origin.

  - **Items** *(string)*

- **`cors_allow_credentials`** *(boolean)*: Indicate that cookies should be supported for cross-origin requests. Defaults to False. Also, cors_allowed_origins cannot be set to ['*'] for credentials to be allowed. The origins must be explicitly specified.

- **`cors_allowed_methods`** *(array)*: A list of HTTP methods that should be allowed for cross-origin requests. Defaults to ['GET']. You can use ['*'] to allow all standard methods.

  - **Items** *(string)*

- **`cors_allowed_headers`** *(array)*: A list of HTTP request headers that should be supported for cross-origin requests. Defaults to []. You can use ['*'] to allow all headers. The Accept, Accept-Language, Content-Language and Content-Type headers are always allowed for CORS requests.

  - **Items** *(string)*

- **`service_name`** *(string)*: Default: `encryption_key_store`.

- **`server_private_key`** *(string)*: Base64 encoded server Crypt4GH private key.

- **`server_public_key`** *(string)*: Base64 encoded server Crypt4GH public key.


### Usage:

A template YAML for configurating the service can be found at
[`./example-config.yaml`](./example-config.yaml).
Please adapt it, rename it to `.ekss.yaml`, and place it into one of the following locations:
- in the current working directory were you are execute the service (on unix: `./.ekss.yaml`)
- in your home directory (on unix: `~/.ekss.yaml`)

The config yaml will be automatically parsed by the service.

**Important: If you are using containers, the locations refer to paths within the container.**

All parameters mentioned in the [`./example-config.yaml`](./example-config.yaml)
could also be set using environment variables or file secrets.

For naming the environment variables, just prefix the parameter name with `ekss_`,
e.g. for the `host` set an environment variable named `ekss_host`
(you may use both upper or lower cases, however, it is standard to define all env
variables in upper cases).

To using file secrets please refer to the
[corresponding section](https://pydantic-docs.helpmanual.io/usage/settings/#secret-support)
of the pydantic documentation.

## HTTP API
An OpenAPI specification for this service can be found [here](./openapi.yaml).

## Architecture and Design:
<!-- Please provide an overview of the architecture and design of the code base.
Mention anything that deviates from the standard triple hexagonal architecture and
the corresponding structure. -->

This is a Python-based service following the Triple Hexagonal Architecture pattern.
It uses protocol/provider pairs and dependency injection mechanisms provided by the
[hexkit](https://github.com/ghga-de/hexkit) library.


## Development
For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of vscode
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as vscode with its "Remote - Containers"
extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in vscode and run the command
`Remote-Containers: Reopen in Container` from the vscode "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant vscode extensions pre-installed
- pre-configured linting and auto-formating
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, a convenience commands `dev_install` is available.
It installs the service with all development dependencies, installs pre-commit.

The installation is performed automatically when you build the devcontainer. However,
if you update dependencies in the [`./setup.cfg`](./setup.cfg) or the
[`./requirements-dev.txt`](./requirements-dev.txt), please run it again.

## License
This repository is free to use and modify according to the
[Apache 2.0 License](./LICENSE).

## Readme Generation
This readme is autogenerate, please see [`readme_generation.md`](./readme_generation.md)
for details.
