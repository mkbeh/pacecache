# OpenTelemetry metrics for pacecache

`paceotel` provides optional OpenTelemetry metrics for [pacecache](https://github.com/mkbeh/pacecache).

The package is exporter-agnostic: applications configure the OpenTelemetry SDK and exporter, while `paceotel` registers
cache metrics with the configured `MeterProvider`.

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

// Configure the OpenTelemetry Meter Provider.
provider := sdkmetric.NewMeterProvider()
defer provider.Shutdown(context.Background())

// Create the reusable OpenTelemetry integration.
metrics := paceotel.New(
    paceotel.WithMeterProvider(provider),
)

// Attach the integration when creating a cache.
cache, _ := pacecache.New[string, string](
    "users",
    pacecache.WithMetrics(metrics),
)
defer cache.Close()

```
<!-- @formatter:on -->

See [example](../../examples/otel) for a runnable example using the OpenTelemetry stdout metric exporter.