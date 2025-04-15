### OpenTelemetry Configuration

OpenTelemetry instrumentation is available via a set of instrumentation libraries and
`opentelemetry-instrument` is used as entrypoint inside the service containers to enable auto instrumentation.
Specific features can be configured on the service level via environment variables as documented in [OpenTelemetry Environment Variable Specification](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/) and [SDK Configuration](https://opentelemetry.io/docs/languages/sdk-configuration/).

Of these the most important variables are `OTEL_EXPORTER_OTLP_PROTOCOL`, which needs to be set to `http/protobuf`.
By default the services send traces to a localhost port, but for actual deployments `OTEL_EXPORTER_OTLP_ENDPOINT` needs to be set, pointing to the correct endpoint.
If OpenTelemetry functionality is not desired for a specific service,  `OTEL_SDK_DISABLED` should be set to `true`, which forces the corresponding service to use no-op implementations instead.
