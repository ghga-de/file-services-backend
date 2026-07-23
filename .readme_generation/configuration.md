### OpenTelemetry Configuration

OpenTelemetry instrumentation is available via a set of instrumentation libraries.
Both the tracer provider and the autoinstrumentation of those libraries are set up
programmatically when the service starts, so no wrapper around the entrypoint is needed
inside the service containers.
Specific features can be configured on the service level via environment variables as documented in [OpenTelemetry Environment Variable Specification](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/) and [SDK Configuration](https://opentelemetry.io/docs/languages/sdk-configuration/).

Some of these are exposed via config options on the service level.
By default, OpenTelemetry is disabled and can be enabled by setting `enable_opentelemetry` to true.
The share of traces that get sampled can be adjusted via the `otel_trace_sampling_rate` config option.
Traces are exported via OTLP over HTTP using the protobuf encoding.
By default the services send traces to a localhost port, but for actual deployments `OTEL_EXPORTER_OTLP_ENDPOINT` needs to be set, pointing to the correct endpoint.
