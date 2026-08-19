package paceotel

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestNew(t *testing.T) {
	metrics := New(nil)
	if metrics == nil {
		t.Fatal("New() = nil")
	}
	if metrics.meterProvider != nil {
		t.Fatalf(
			"New() meterProvider = %T, want nil",
			metrics.meterProvider,
		)
	}
}

func TestWithMeterProvider(t *testing.T) {
	provider := sdkmetric.NewMeterProvider()
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	})

	metrics := New(
		WithMeterProvider(provider),
	)

	if metrics.meterProvider != provider {
		t.Fatalf(
			"New() meterProvider = %T, want provided MeterProvider",
			metrics.meterProvider,
		)
	}
}

func TestWithMeterProviderNil(t *testing.T) {
	metrics := New(
		WithMeterProvider(nil),
	)

	if metrics.meterProvider != nil {
		t.Fatalf(
			"New() meterProvider = %T, want nil",
			metrics.meterProvider,
		)
	}
}
