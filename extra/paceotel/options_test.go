package paceotel

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestWithMeterProvider(t *testing.T) {
	provider := sdkmetric.NewMeterProvider()
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	var settings settings
	WithMeterProvider(provider)(&settings)

	if settings.meterProvider != provider {
		t.Fatalf(
			"WithMeterProvider() provider = %T, want provided MeterProvider",
			settings.meterProvider,
		)
	}
}

func TestWithMeterProviderNil(t *testing.T) {
	var settings settings
	WithMeterProvider(nil)(&settings)

	if settings.meterProvider != nil {
		t.Fatalf(
			"WithMeterProvider(nil) provider = %T, want nil",
			settings.meterProvider,
		)
	}
}
