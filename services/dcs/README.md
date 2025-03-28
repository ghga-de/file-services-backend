# Download Controller Service

a GA4GH DRS-compliant service for delivering files from S3 encrypted according to the GA4GH Crypt4GH standard.

## Description

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
docker pull ghga/download-controller-service:5.0.0
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/download-controller-service:5.0.0 .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/download-controller-service:5.0.0 --help
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
- **`file_deletion_request_topic`** *(string, required)*: The name of the topic to receive events informing about files to delete.


  Examples:

  ```json
  "file-deletion-requests"
  ```


- **`file_deletion_request_type`** *(string, required)*: The type used for events indicating that a request to delete a file has been received.


  Examples:

  ```json
  "file_deletion_requested"
  ```


- **`log_level`** *(string)*: The minimum log level to capture. Must be one of: `["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "TRACE"]`. Default: `"INFO"`.

- **`service_name`** *(string)*: Default: `"dcs"`.

- **`service_instance_id`** *(string, required)*: A string that uniquely identifies this instance across all instances of this service. A globally unique Kafka client ID will be created by concatenating the service_name and the service_instance_id.


  Examples:

  ```json
  "germany-bw-instance-001"
  ```


- **`log_format`**: If set, will replace JSON formatting with the specified string format. If not set, has no effect. In addition to the standard attributes, the following can also be specified: timestamp, service, instance, level, correlation_id, and details. Default: `null`.

  - **Any of**

    - *string*

    - *null*


  Examples:

  ```json
  "%(timestamp)s - %(service)s - %(level)s - %(message)s"
  ```


  ```json
  "%(asctime)s - Severity: %(levelno)s - %(msg)s"
  ```


- **`log_traceback`** *(boolean)*: Whether to include exception tracebacks in log messages. Default: `true`.

- **`object_storages`** *(object, required)*: Can contain additional properties.

  - **Additional properties**: Refer to *[#/$defs/S3ObjectStorageNodeConfig](#%24defs/S3ObjectStorageNodeConfig)*.

- **`file_internally_registered_topic`** *(string, required)*: Name of the topic used for events indicating that a file has been registered for download.


  Examples:

  ```json
  "file-registrations"
  ```


  ```json
  "file-registrations-internal"
  ```


- **`file_internally_registered_type`** *(string, required)*: The type used for event indicating that that a file has been registered for download.


  Examples:

  ```json
  "file_internally_registered"
  ```


- **`files_to_stage_topic`** *(string, required)*: Name of the topic used for events indicating that a download was requested for a file that is not yet available in the outbox.


  Examples:

  ```json
  "file-staging-requests"
  ```


- **`files_to_stage_type`** *(string, required)*: The type used for non-staged file request events.


  Examples:

  ```json
  "file_staging_requested"
  ```


- **`file_registered_for_download_topic`** *(string, required)*: Name of the topic used for events indicating that a file has been registered by the DCS for download.


  Examples:

  ```json
  "file-registrations"
  ```


  ```json
  "file-registrations-download"
  ```


- **`file_registered_for_download_type`** *(string, required)*: The type used for event indicating that a file has been registered by the DCS for download.


  Examples:

  ```json
  "file_registered_for_download"
  ```


- **`file_deleted_topic`** *(string, required)*: Name of the topic used for events indicating that a file has been deleted.


  Examples:

  ```json
  "file-deletions"
  ```


- **`file_deleted_type`** *(string, required)*: The type used for events indicating that a file has been deleted.


  Examples:

  ```json
  "file_deleted"
  ```


- **`download_served_topic`** *(string, required)*: Name of the topic used for events indicating that a download of a specified file happened.


  Examples:

  ```json
  "file-downloads"
  ```


- **`download_served_type`** *(string, required)*: The type used for event indicating that a download of a specified file happened.


  Examples:

  ```json
  "download_served"
  ```


- **`kafka_servers`** *(array, required)*: A list of connection strings to connect to Kafka bootstrap servers.

  - **Items** *(string)*


  Examples:

  ```json
  [
      "localhost:9092"
  ]
  ```


- **`kafka_security_protocol`** *(string)*: Protocol used to communicate with brokers. Valid values are: PLAINTEXT, SSL. Must be one of: `["PLAINTEXT", "SSL"]`. Default: `"PLAINTEXT"`.

- **`kafka_ssl_cafile`** *(string)*: Certificate Authority file path containing certificates used to sign broker certificates. If a CA is not specified, the default system CA will be used if found by OpenSSL. Default: `""`.

- **`kafka_ssl_certfile`** *(string)*: Optional filename of client certificate, as well as any CA certificates needed to establish the certificate's authenticity. Default: `""`.

- **`kafka_ssl_keyfile`** *(string)*: Optional filename containing the client private key. Default: `""`.

- **`kafka_ssl_password`** *(string, format: password, write-only)*: Optional password to be used for the client private key. Default: `""`.

- **`generate_correlation_id`** *(boolean)*: A flag, which, if False, will result in an error when inbound requests don't possess a correlation ID. If True, requests without a correlation ID will be assigned a newly generated ID in the correlation ID middleware function. Default: `true`.


  Examples:

  ```json
  true
  ```


  ```json
  false
  ```


- **`kafka_max_message_size`** *(integer)*: The largest message size that can be transmitted, in bytes. Only services that have a need to send/receive larger messages should set this. Exclusive minimum: `0`. Default: `1048576`.


  Examples:

  ```json
  1048576
  ```


  ```json
  16777216
  ```


- **`kafka_max_retries`** *(integer)*: The maximum number of times to immediately retry consuming an event upon failure. Works independently of the dead letter queue. Minimum: `0`. Default: `0`.


  Examples:

  ```json
  0
  ```


  ```json
  1
  ```


  ```json
  2
  ```


  ```json
  3
  ```


  ```json
  5
  ```


- **`kafka_enable_dlq`** *(boolean)*: A flag to toggle the dead letter queue. If set to False, the service will crash upon exhausting retries instead of publishing events to the DLQ. If set to True, the service will publish events to the DLQ topic after exhausting all retries. Default: `false`.


  Examples:

  ```json
  true
  ```


  ```json
  false
  ```


- **`kafka_dlq_topic`** *(string)*: The name of the topic used to resolve error-causing events. Default: `"dlq"`.


  Examples:

  ```json
  "dlq"
  ```


- **`kafka_retry_backoff`** *(integer)*: The number of seconds to wait before retrying a failed event. The backoff time is doubled for each retry attempt. Minimum: `0`. Default: `0`.


  Examples:

  ```json
  0
  ```


  ```json
  1
  ```


  ```json
  2
  ```


  ```json
  3
  ```


  ```json
  5
  ```


- **`mongo_dsn`** *(string, format: multi-host-uri, required)*: MongoDB connection string. Might include credentials. For more information see: https://naiveskill.com/mongodb-connection-string/. Length must be at least 1.


  Examples:

  ```json
  "mongodb://localhost:27017"
  ```


- **`db_name`** *(string, required)*: Name of the database located on the MongoDB server.


  Examples:

  ```json
  "my-database"
  ```


- **`mongo_timeout`**: Timeout in seconds for API calls to MongoDB. The timeout applies to all steps needed to complete the operation, including server selection, connection checkout, serialization, and server-side execution. When the timeout expires, PyMongo raises a timeout exception. If set to None, the operation will not time out (default MongoDB behavior). Default: `null`.

  - **Any of**

    - *integer*: Exclusive minimum: `0`.

    - *null*


  Examples:

  ```json
  300
  ```


  ```json
  600
  ```


  ```json
  null
  ```


- **`drs_server_uri`** *(string, required)*: The base of the DRS URI to access DRS objects. Has to start with 'drs://' and end with '/'.


  Examples:

  ```json
  "drs://localhost:8080/"
  ```


- **`staging_speed`** *(integer)*: When trying to access a DRS object that is not yet in the outbox, assume that this many megabytes can be staged per second. Default: `100`.


  Examples:

  ```json
  100
  ```


  ```json
  500
  ```


- **`retry_after_min`** *(integer)*: When trying to access a DRS object that is not yet in the outbox, wait at least this number of seconds before trying again. Default: `5`.


  Examples:

  ```json
  5
  ```


  ```json
  10
  ```


- **`retry_after_max`** *(integer)*: When trying to access a DRS object that is not yet in the outbox, wait at most this number of seconds before trying again. Default: `300`.


  Examples:

  ```json
  30
  ```


  ```json
  300
  ```


- **`ekss_base_url`** *(string, required)*: URL containing host and port of the EKSS endpoint to retrieve personalized envelope from.


  Examples:

  ```json
  "http://ekss:8080/"
  ```


- **`presigned_url_expires_after`** *(integer, required)*: Expiration time in seconds for presigned URLS. Positive integer required. Exclusive minimum: `0`.


  Examples:

  ```json
  30
  ```


  ```json
  60
  ```


- **`http_call_timeout`** *(integer)*: Time in seconds after which http calls from this service should timeout. Exclusive minimum: `0`. Default: `3`.


  Examples:

  ```json
  1
  ```


  ```json
  5
  ```


  ```json
  60
  ```


- **`outbox_cache_timeout`** *(integer)*: Time in days since last access after which a file present in the outbox should be unstaged and has to be requested from permanent storage again for the next request. Default: `7`.


  Examples:

  ```json
  7
  ```


  ```json
  30
  ```


- **`auth_key`** *(string, required)*: The GHGA internal public key for validating the token signature.


  Examples:

  ```json
  "{\"crv\": \"P-256\", \"kty\": \"EC\", \"x\": \"...\", \"y\": \"...\"}"
  ```


- **`auth_algs`** *(array)*: A list of all algorithms used for signing GHGA internal tokens. Default: `["ES256"]`.

  - **Items** *(string)*

- **`auth_check_claims`** *(object)*: A dict of all GHGA internal claims that shall be verified. Default: `{"type": null, "file_id": null, "user_id": null, "user_public_crypt4gh_key": null, "full_user_name": null, "email": null, "iat": null, "exp": null}`.

- **`auth_map_claims`** *(object)*: A mapping of claims to attributes in the GHGA auth context. Can contain additional properties. Default: `{}`.

  - **Additional properties** *(string)*

- **`host`** *(string)*: IP of the host. Default: `"127.0.0.1"`.

- **`port`** *(integer)*: Port to expose the server on the specified host. Default: `8080`.

- **`auto_reload`** *(boolean)*: A development feature. Set to `True` to automatically reload the server upon code changes. Default: `false`.

- **`workers`** *(integer)*: Number of workers processes to run. Default: `1`.

- **`api_root_path`** *(string)*: Root path at which the API is reachable. This is relative to the specified host and port. Default: `""`.

- **`openapi_url`** *(string)*: Path to get the openapi specification in JSON format. This is relative to the specified host and port. Default: `"/openapi.json"`.

- **`docs_url`** *(string)*: Path to host the swagger documentation. This is relative to the specified host and port. Default: `"/docs"`.

- **`cors_allowed_origins`**: A list of origins that should be permitted to make cross-origin requests. By default, cross-origin requests are not allowed. You can use ['*'] to allow any origin. Default: `null`.

  - **Any of**

    - *array*

      - **Items** *(string)*

    - *null*


  Examples:

  ```json
  [
      "https://example.org",
      "https://www.example.org"
  ]
  ```


- **`cors_allow_credentials`**: Indicate that cookies should be supported for cross-origin requests. Defaults to False. Also, cors_allowed_origins cannot be set to ['*'] for credentials to be allowed. The origins must be explicitly specified. Default: `null`.

  - **Any of**

    - *boolean*

    - *null*


  Examples:

  ```json
  [
      "https://example.org",
      "https://www.example.org"
  ]
  ```


- **`cors_allowed_methods`**: A list of HTTP methods that should be allowed for cross-origin requests. Defaults to ['GET']. You can use ['*'] to allow all standard methods. Default: `null`.

  - **Any of**

    - *array*

      - **Items** *(string)*

    - *null*


  Examples:

  ```json
  [
      "*"
  ]
  ```


- **`cors_allowed_headers`**: A list of HTTP request headers that should be supported for cross-origin requests. Defaults to []. You can use ['*'] to allow all headers. The Accept, Accept-Language, Content-Language and Content-Type headers are always allowed for CORS requests. Default: `null`.

  - **Any of**

    - *array*

      - **Items** *(string)*

    - *null*


  Examples:

  ```json
  []
  ```


- **`api_route`** *(string)*: DRS API route. Default: `"/ga4gh/drs/v1"`.

## Definitions


- <a id="%24defs/S3Config"></a>**`S3Config`** *(object)*: S3-specific config params.
Inherit your config class from this class if you need
to talk to an S3 service in the backend.<br>  Args:
    s3_endpoint_url (str): The URL to the S3 endpoint.
    s3_access_key_id (str):
        Part of credentials for login into the S3 service. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    s3_secret_access_key (str):
        Part of credentials for login into the S3 service. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    s3_session_token (Optional[str]):
        Optional part of credentials for login into the S3 service. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    aws_config_ini (Optional[Path]):
        Path to a config file for specifying more advanced S3 parameters.
        This should follow the format described here:
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file
        Defaults to None. Cannot contain additional properties.

  - **`s3_endpoint_url`** *(string, required)*: URL to the S3 API.


    Examples:

    ```json
    "http://localhost:4566"
    ```


  - **`s3_access_key_id`** *(string, required)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.


    Examples:

    ```json
    "my-access-key-id"
    ```


  - **`s3_secret_access_key`** *(string, format: password, required, write-only)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.


    Examples:

    ```json
    "my-secret-access-key"
    ```


  - **`s3_session_token`**: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html. Default: `null`.

    - **Any of**

      - *string, format: password*

      - *null*


    Examples:

    ```json
    "my-session-token"
    ```


  - **`aws_config_ini`**: Path to a config file for specifying more advanced S3 parameters. This should follow the format described here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file. Default: `null`.

    - **Any of**

      - *string, format: path*

      - *null*


    Examples:

    ```json
    "~/.aws/config"
    ```


- <a id="%24defs/S3ObjectStorageNodeConfig"></a>**`S3ObjectStorageNodeConfig`** *(object)*: Configuration for one specific object storage node and one bucket in it.<br>  The bucket is the main bucket that the service is responsible for. Cannot contain additional properties.

  - **`bucket`** *(string, required)*

  - **`credentials`**: Refer to *[#/$defs/S3Config](#%24defs/S3Config)*.


### Usage:

A template YAML for configuring the service can be found at
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
An OpenAPI specification for this service can be found [here](openapi.yaml).

## Architecture and Design:
<!-- Please provide an overview of the architecture and design of the code base.
Mention anything that deviates from the standard triple hexagonal architecture and
the corresponding structure. -->

This is a Python-based service following the Triple Hexagonal Architecture pattern.
It uses protocol/provider pairs and dependency injection mechanisms provided by the
[hexkit](https://github.com/ghga-de/hexkit) library.


## Development

For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of VS Code
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as VS Code with its "Remote - Containers"
extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in VS Code and run the command
`Remote-Containers: Reopen in Container` from the VS Code "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant VS Code extensions pre-installed
- pre-configured linting and auto-formatting
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, a convenience commands `dev_install` is available.
It installs the service with all development dependencies, installs pre-commit.

The installation is performed automatically when you build the devcontainer. However,
if you update dependencies in the [`./pyproject.toml`](./pyproject.toml) or the
[`./requirements-dev.txt`](./requirements-dev.txt), please run it again.

## License

This repository is free to use and modify according to the
[Apache 2.0 License](./LICENSE).

## README Generation

This README file is auto-generated, please see [`readme_generation.md`](./readme_generation.md)
for details.
