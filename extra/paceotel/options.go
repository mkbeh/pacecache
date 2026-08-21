package paceotel

import "go.opentelemetry.io/otel/metric"

// Option configures OpenTelemetry metrics.
type Option func(*settings)

type settings struct {
	meterProvider metric.MeterProvider
}

// New creates an OpenTelemetry metrics implementation.
//
// By default, metrics use the global OpenTelemetry MeterProvider. The returned
// value is immutable and may be reused across multiple caches.
func New(options ...Option) *Metrics {
	settings := settings{}

	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}

	return &Metrics{
		meterProvider: settings.meterProvider,
	}
}

// WithMeterProvider configures the MeterProvider used for cache metrics.
//
// The caller owns the provider and is responsible for its lifecycle.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(settings *settings) {
		settings.meterProvider = provider
	}
}
