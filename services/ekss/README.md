# Encryption Key Store Service

providing crypt4gh file secret extraction, storage and envelope generation

## Description

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

### Vault configuration:

For the aforementioned endpoints to work correctly, the vault instance the encryption
key store communicates with needs to set policies granting *create* and *read* privileges
on all secret paths managed and *delete* priviliges on the respective metadata.

For all encryption keys stored under a prefix of *ekss* this might look like
```
path "secret/data/ekss/*" {
    capabilities = ["read", "create"]
}
path "secret/metadata/ekss/*" {
    capabilities = ["delete"]
}
```


## Installation

We recommend using the provided Docker container.

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/encryption-key-store-service):
```bash
docker pull ghga/encryption-key-store-service:2.0.0
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/encryption-key-store-service:2.0.0 .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/encryption-key-store-service:2.0.0 --help
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
- **`server_private_key_path`** *(string, format: path, required)*: Path to the Crypt4GH private key file.


  Examples:

  ```json
  "./key.sec"
  ```


- **`server_public_key_path`** *(string, format: path, required)*: Path to the Crypt4GH public key file.


  Examples:

  ```json
  "./key.pub"
  ```


- **`private_key_passphrase`**: Passphrase needed to read the content of the private key file. Only needed if the private key is encrypted. Default: `null`.

  - **Any of**

    - *string*

    - *null*

- **`log_level`** *(string)*: The minimum log level to capture. Must be one of: `["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "TRACE"]`. Default: `"INFO"`.

- **`service_name`** *(string)*: Default: `"encryption_key_store"`.

- **`service_instance_id`** *(string, required)*: A string that uniquely identifies this instance across all instances of this service. This is included in log messages.


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

- **`vault_url`** *(string, required)*: URL of the vault instance to connect to.


  Examples:

  ```json
  "http://127.0.0.1.8200"
  ```


- **`vault_role_id`**: Vault role ID to access a specific prefix. Default: `null`.

  - **Any of**

    - *string, format: password*

    - *null*


  Examples:

  ```json
  "example_role"
  ```


- **`vault_secret_id`**: Vault secret ID to access a specific prefix. Default: `null`.

  - **Any of**

    - *string, format: password*

    - *null*


  Examples:

  ```json
  "example_secret"
  ```


- **`vault_verify`**: SSL certificates (CA bundle) used to verify the identity of the vault, or True to use the default CAs, or False for no verification. Default: `true`.

  - **Any of**

    - *boolean*

    - *string*


  Examples:

  ```json
  "/etc/ssl/certs/my_bundle.pem"
  ```


- **`vault_path`** *(string, required)*: Path without leading or trailing slashes where secrets should be stored in the vault.

- **`vault_secrets_mount_point`** *(string)*: Name used to address the secret engine under a custom mount path. Default: `"secret"`.


  Examples:

  ```json
  "secret"
  ```


- **`vault_kube_role`**: Vault role name used for Kubernetes authentication. Default: `null`.

  - **Any of**

    - *string*

    - *null*


  Examples:

  ```json
  "file-ingest-role"
  ```


- **`vault_auth_mount_point`**: Adapter specific mount path for the corresponding auth backend. If none is provided, the default is used. Default: `null`.

  - **Any of**

    - *string*

    - *null*


  Examples:

  ```json
  null
  ```


  ```json
  "approle"
  ```


  ```json
  "kubernetes"
  ```


- **`service_account_token_path`** *(string, format: path)*: Path to service account token used by kube auth adapter. Default: `"/var/run/secrets/kubernetes.io/serviceaccount/token"`.

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


- **`generate_correlation_id`** *(boolean)*: A flag, which, if False, will result in an error when inbound requests don't possess a correlation ID. If True, requests without a correlation ID will be assigned a newly generated ID in the correlation ID middleware function. Default: `true`.


  Examples:

  ```json
  true
  ```


  ```json
  false
  ```



### Usage:

A template YAML for configuring the service can be found at
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
An OpenAPI specification for this service can be found [here](openapi.yaml).

## Architecture and Design:
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
