package paceotel

import (
	"fmt"

	"github.com/mkbeh/pacecache"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/mkbeh/pacecache/extra/paceotel"

// Metrics exports pacecache statistics through OpenTelemetry.
//
// Metrics is immutable and safe to reuse across multiple caches.
type Metrics struct {
	meterProvider metric.MeterProvider
}

var _ pacecache.Metrics = (*Metrics)(nil)

type metricsRegistration struct {
	registration metric.Registration
}

func (registration *metricsRegistration) Close() {
	if registration == nil || registration.registration == nil {
		return
	}

	if err := registration.registration.Unregister(); err != nil {
		otel.Handle(
			fmt.Errorf("paceotel: unregister metrics: %w", err),
		)
	}
}
