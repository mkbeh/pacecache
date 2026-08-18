# OpenTelemetry metrics

This example shows how to integrate `pacecache` with OpenTelemetry metrics using the optional `extra/paceotel` package.

The example uses the stdout exporter for simplicity, while `paceotel` remains exporter-agnostic and works with any
OpenTelemetry SDK exporter configured by the application.

**It demonstrates:**

- Configuring OpenTelemetry metrics with a custom `MeterProvider`
- Registering `paceotel` metrics for a cache
- Observing positive hits, negative hits, loads, misses, and invalidation
- Flushing metrics before a short-lived process exits
- Managing cache and OpenTelemetry lifecycle correctly

## Run

From this example directory:

```bash
go run .
```

Or from the repository root:

```bash
go run ./examples/otel
```

## Metrics

The stdout exporter prints the collected OpenTelemetry metrics as JSON.

Metrics are associated with the logical cache name:

```text
pacecache.name = users
````

and use the instrumentation scope:

```text
github.com/mkbeh/pacecache/extra/paceotel
```

The example generates metrics for cache size and capacity, lookups, loads, invalidation, eviction, and expiration.

Lookup and load metrics include result attributes such as `hit`, `miss`, `negative_hit`, `found`, `not_found`, and
`error`.

Because the example is short-lived, it calls `ForceFlush` before exiting. Long-running applications normally export
metrics periodically through the configured OpenTelemetry metric reader.
