# OpenTelemetry Metrics for pacecache

`paceotel` provides optional OpenTelemetry metrics integration for `pacecache`.

The package is exporter-agnostic: applications own the OpenTelemetry SDK lifecycle and exporter configuration, while
`paceotel` uses the configured `MeterProvider` to expose cache metrics.

## Installation

```bash
go get github.com/mkbeh/pacecache/extra/paceotel
```

## Usage

<!-- @formatter:off -->
```go
import (
    "context"

    "github.com/mkbeh/pacecache"
    "github.com/mkbeh/pacecache/extra/paceotel"
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

provider := sdkmetric.NewMeterProvider()
defer provider.Shutdown(context.Background())

// Create a reusable OpenTelemetry metrics integration.
metrics := paceotel.New(
    paceotel.WithMeterProvider(provider),
)

// Attach metrics when creating the cache.
cache, _ := pacecache.New[string, string](
    "users",
    pacecache.WithMetrics(metrics),
)
defer cache.Close()
```
<!-- @formatter:on -->

For a complete runnable setup using the stdout exporter, see the [example](../../examples/otel).