### OpenTelemetry Configuration

OpenTelemetry instrumentation is available via a set of instrumentation libraries and
`opentelemetry-instrument` is used as entrypoint inside the service containers to enable auto instrumentation.
Specific features can be configured on the service level via environment variables as documented in [OpenTelemetry Environment Variable Specification](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/) and [SDK Configuration](https://opentelemetry.io/docs/languages/sdk-configuration/).

Some of these are exposed via config options on the service level.
By default, OpenTelemetry is disabled and can be enabled by setting `enable_opentelemetry` to true.
Contrary to the opentelemetry-distro default, `OTEL_EXPORTER_OTLP_PROTOCOL` is set to `http/protobuf` and can be changed
using the `otel_exporter_protocol` config option.
By default the services send traces to a localhost port, but for actual deployments `OTEL_EXPORTER_OTLP_ENDPOINT` needs to be set, pointing to the correct endpoint.
