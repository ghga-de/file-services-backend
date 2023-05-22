
[![tests](https://github.com/ghga-de/download-controller-service/actions/workflows/unit_and_int_tests.yaml/badge.svg)](https://github.com/ghga-de/download-controller-service/actions/workflows/unit_and_int_tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/download-controller-service/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/download-controller-service?branch=main)

# Download Controller Service

Download Controller Service - a GA4GH DRS-compliant service for delivering files from S3 encrypted according to the GA4GH Crypt4GH standard.

## Description

<!-- Please provide a short overview of the features of this service.-->

This service implements the
[GA4GH DRS](https://github.com/ga4gh/data-repository-service-schemas) v1.0.0 for
serving files that where encrypted according to the
[GA4GH Crypt4GH](https://www.ga4gh.org/news/crypt4gh-a-secure-method-for-sharing-human-genetic-data/)
from S3-compatible object storages.

Thereby, only the `GET /objects/{object_id}` is implemented. It always returns
an access_method for the object via S3. This makes the second endpoint
`GET /objects/{object_id}/access/{access_id}` that
is contained in the DRS spec unnecessary. For more details see the OpenAPI spec
described below.

For authorization, a JSON web token is expected via Bearer Authentication that has a format
described [here](./dcs/core/auth_policies.py).

All files that can be requested are registered in a MongoDB database owned and
controlled by this service. Registration of new events happens through a Kafka event.

It serves pre-signed URLs to S3 objects located in a single so-called outbox bucket.
If the file is not already in the bucket when the user calls the object endpoint,
an event is published to request staging the file to the outbox. The staging has to
be carried out by a different service.

For more details on the events consumed and produced by this service, see the
configuration.

The DRS object endpoint serves files in an encrypted fashion as described by the
Crypt4GH standard, but without the evelope. A user-specific envelope can be requested
from the `GET /objects/{object_id}/envelopes` endpoint. The actual envelope creation
is delegated to another service via a RESTful call. Please see the configuration for
further details.


## Installation
We recommend using the provided Docker container.

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/download-controller-service):
```bash
docker pull ghga/download-controller-service:0.5.9
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/download-controller-service:0.5.9 .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/download-controller-service:0.5.9 --help
```

If you prefer not to use containers, you may install the service from source:
```bash
# Execute in the repo's root dir:
pip install .

# To run the service:
dcs --help
```

## Configuration
### Parameters

The service requires the following configuration parameters:
- **`files_to_register_topic`** *(string)*: The name of the topic to receive events informing about new files that shall be made available for download.

- **`files_to_register_type`** *(string)*: The type used for events informing about new files that shall be made available for download.

- **`download_served_event_topic`** *(string)*: Name of the topic used for events indicating that a download of a specified file happened.

- **`download_served_event_type`** *(string)*: The type used for event indicating that a download of a specified file happened.

- **`unstaged_download_event_topic`** *(string)*: Name of the topic used for events indicating that a download was requested for a file that is not yet available in the outbox.

- **`unstaged_download_event_type`** *(string)*: The type used for event indicating that a download was requested for a file that is not yet available in the outbox.

- **`file_registered_event_topic`** *(string)*: Name of the topic used for events indicating that a file has been registered for download.

- **`file_registered_event_type`** *(string)*: The type used for event indicating that that a file has been registered for download.

- **`service_name`** *(string)*: Default: `dcs`.

- **`service_instance_id`** *(string)*: A string that uniquely identifies this instance across all instances of this service. A globally unique Kafka client ID will be created by concatenating the service_name and the service_instance_id.

- **`kafka_servers`** *(array)*: A list of connection strings to connect to Kafka bootstrap servers.

  - **Items** *(string)*

- **`db_connection_str`** *(string)*: MongoDB connection string. Might include credentials. For more information see: https://naiveskill.com/mongodb-connection-string/.

- **`db_name`** *(string)*: Name of the database located on the MongoDB server.

- **`outbox_bucket`** *(string)*

- **`drs_server_uri`** *(string)*: The base of the DRS URI to access DRS objects. Has to start with 'drs://' and end with '/'.

- **`retry_access_after`** *(integer)*: When trying to access a DRS object that is not yet in the outbox, instruct to retry after this many seconds. Default: `120`.

- **`ekss_base_url`** *(string)*: URL containing host and port of the EKSS endpoint to retrieve personalized envelope from.

- **`presigned_url_expires_after`** *(integer)*: Expiration time in seconds for presigned URLS. Positive integer required.

- **`s3_endpoint_url`** *(string)*: URL to the S3 API.

- **`s3_access_key_id`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.

- **`s3_secret_access_key`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.

- **`s3_session_token`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.

- **`aws_config_ini`** *(string)*: Path to a config file for specifying more advanced S3 parameters. This should follow the format described here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file.

- **`auth_key`** *(string)*: The GHGA internal public key for validating the token signature.

- **`auth_algs`** *(array)*: A list of all algorithms used for signing GHGA internal tokens. Default: `['ES256']`.

  - **Items** *(string)*

- **`auth_check_claims`** *(object)*: A dict of all GHGA internal claims that shall be verified. Default: `{'name': None, 'email': None, 'iat': None, 'exp': None}`.

- **`auth_map_claims`** *(object)*: A mapping of claims to attributes in the GHGA auth context. Can contain additional properties. Default: `{}`.

  - **Additional Properties** *(string)*

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

- **`api_route`** *(string)*: Default: `/ga4gh/drs/v1`.


### Usage:

A template YAML for configurating the service can be found at
[`./example-config.yaml`](./example-config.yaml).
Please adapt it, rename it to `.dcs.yaml`, and place it into one of the following locations:
- in the current working directory were you are execute the service (on unix: `./.dcs.yaml`)
- in your home directory (on unix: `~/.dcs.yaml`)

The config yaml will be automatically parsed by the service.

**Important: If you are using containers, the locations refer to paths within the container.**

All parameters mentioned in the [`./example-config.yaml`](./example-config.yaml)
could also be set using environment variables or file secrets.

For naming the environment variables, just prefix the parameter name with `dcs_`,
e.g. for the `host` set an environment variable named `dcs_host`
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
