# OpenTelemetry Metrics

This example demonstrates how to integrate `pacecache` with OpenTelemetry using the optional `paceotel` package.

It uses the stdout exporter for a minimal setup. `paceotel` is exporter-agnostic, so the application can use any
OpenTelemetry SDK exporter without changing the cache integration.

## Key Concepts Covered

* Configuring OpenTelemetry with a custom `MeterProvider`
* Attaching `paceotel` to a cache through `pacecache.WithMetrics`
* Observing hits, misses, loader outcomes, and deletions
* Handling not-found loader results without caching them
* Flushing telemetry before a short-lived process exits
* Managing cache and OpenTelemetry lifecycles correctly

## Run

Run the example from this directory:

```bash
go run .
```

Or from the repository root:

```bash
go run ./examples/otel
```

## Metrics Specification

The example uses the stdout exporter to emit collected OpenTelemetry metrics as JSON. Metrics are associated with the
logical cache instance through the `pacecache.name` attribute, while the integration records cache size, capacity,
lookups, loader outcomes, deletions, cleanup activity, evictions, and expirations.

Metrics include attributes for distinguishing operation outcomes. The `pacecache.lookup.result` attribute identifies
cache lookups as `hit` or `miss`, while `pacecache.load.result` identifies loader outcomes as `found`, `not_found`, or
`error`. The `pacecache.entry.removed.count` metric uses `pacecache.removal.operation` with `delete` and `clear` to
distinguish entries removed by key-scoped deletion from entries removed by `Clear`.

The exported metrics also reflect the cache-aside behavior of missing results. Requesting user `404` twice produces two
cache misses and two `not_found` loader outcomes because results returned with `found=false` are not stored in the
cache. Each subsequent lookup therefore invokes the repository again.

Because this example is short-lived, it calls `ForceFlush` before exiting to flush pending telemetry. Long-running
applications normally rely on their configured OpenTelemetry metric reader to collect and export metrics continuously.
