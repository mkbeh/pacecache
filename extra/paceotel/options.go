package paceotel

import "go.opentelemetry.io/otel/metric"

// Option configures OpenTelemetry metrics.
type Option func(*settings)

type settings struct {
	meterProvider metric.MeterProvider
}

// WithMeterProvider configures the MeterProvider used for cache metrics.
//
// The caller owns the provider and is responsible for its lifecycle.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(settings *settings) {
		settings.meterProvider = provider
	}
}
