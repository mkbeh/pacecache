# OpenTelemetry metrics

This example shows how to integrate `pacecache` with OpenTelemetry metrics using the optional `extra/paceotel` package.

The example uses the stdout exporter for simplicity, while `paceotel` remains exporter-agnostic and works with any
OpenTelemetry SDK exporter configured by the application.

**It demonstrates:**

- Configuring OpenTelemetry metrics with a custom `MeterProvider`
- Attaching `paceotel` metrics to a cache through `pacecache.WithMetrics`
- Observing cache hits and misses, loader results, and invalidation
- Returning not-found loader results without storing them in the cache
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
```

and use the instrumentation scope:

```text
github.com/mkbeh/pacecache/extra/paceotel
```

The integration exposes metrics for cache size and capacity, lookups, loads, invalidation, cleanup, eviction, and
expiration.

Lookup metrics use `lookup.result` values such as `hit` and `miss`. Load metrics use `load.result` values such as
`found`, `not_found`, and `error`.

In this example, user `404` is intentionally loaded twice. Because a `found=false` loader result is not stored by
`pacecache`, both lookups reach the repository and are reported as misses with `not_found` load results.

Because the example is short-lived, it calls `ForceFlush` before exiting. Long-running applications normally export
metrics periodically through the configured OpenTelemetry metric reader.
