package paceotel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mkbeh/pacecache"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Source exposes the cache identity and statistics required by Metrics.
type Source interface {
	Name() string
	Stats() pacecache.Stats
}

// Metrics exports pacecache statistics through OpenTelemetry.
//
// Metrics is safe to reuse across multiple caches. Each registered source must
// have a unique name within a Metrics instance; only one unnamed source may be
// registered. Unregister removes the OpenTelemetry callbacks for all sources.
type Metrics struct {
	meter metric.Meter

	mu            sync.Mutex
	instruments   *metricInstruments
	registrations map[string]metric.Registration
	closed        bool
}

// New creates an OpenTelemetry metrics implementation.
//
// By default, metrics use the global OpenTelemetry MeterProvider. The returned
// value may be reused across multiple caches. Call Unregister to remove its
// OpenTelemetry callbacks and release references to registered sources.
func New(options ...Option) *Metrics {
	settings := settings{}

	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}

	meterProvider := settings.meterProvider
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}

	return &Metrics{
		meter: meterProvider.Meter(
			ScopeName,
			metric.WithInstrumentationVersion(Version()),
		),
		registrations: make(map[string]metric.Registration),
	}
}

// Register adds one cache metrics source to this OpenTelemetry integration.
// The source name must be unique within Metrics. An empty name is allowed, but
// only one unnamed source may be registered. Additional attributes are attached
// to every metric observation for the source. paceotel-owned attributes take
// precedence over attributes with the same keys.
func (metrics *Metrics) Register(source Source, attributes ...attribute.KeyValue) error {
	if metrics == nil {
		return errors.New("paceotel: metrics is nil")
	}

	if source == nil {
		return errors.New("paceotel: source is nil")
	}

	name := source.Name()

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if metrics.closed {
		return errors.New("paceotel: metrics is unregistered")
	}

	if _, exists := metrics.registrations[name]; exists {
		return fmt.Errorf("paceotel: metrics source %q already registered", name)
	}

	if metrics.instruments == nil {
		instruments, err := newMetricInstruments(metrics.meter)
		if err != nil {
			return err
		}

		metrics.instruments = &instruments
	}

	registration, err := metrics.registerCallback(
		source,
		newMetricAttributes(name, attributes...),
	)
	if err != nil {
		return err
	}

	metrics.registrations[name] = registration

	return nil
}

// Unregister removes the OpenTelemetry callbacks associated with Metrics and
// releases references to all successfully unregistered sources. Repeated calls
// are safe and retry callbacks that previously failed to unregister.
func (metrics *Metrics) Unregister() error {
	if metrics == nil {
		return nil
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	metrics.closed = true
	metrics.instruments = nil

	var unregisterErr error

	for name, registration := range metrics.registrations {
		if err := registration.Unregister(); err != nil {
			unregisterErr = errors.Join(
				unregisterErr,
				fmt.Errorf("source %q: %w", name, err),
			)
			continue
		}

		delete(metrics.registrations, name)
	}

	if len(metrics.registrations) == 0 {
		metrics.registrations = nil
	}

	if unregisterErr != nil {
		return fmt.Errorf("paceotel: unregister metrics: %w", unregisterErr)
	}

	return nil
}

func (metrics *Metrics) registerCallback(source Source, attributes metricAttributes) (metric.Registration, error) {
	instruments := *metrics.instruments

	registration, err := metrics.meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			instruments.observe(
				observer,
				source.Stats(),
				attributes,
			)

			return nil
		},
		instruments.observables()...,
	)
	if err != nil {
		return nil, fmt.Errorf("paceotel: register metrics callback: %w", err)
	}

	return registration, nil
}
