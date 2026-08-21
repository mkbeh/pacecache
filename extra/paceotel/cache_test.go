package paceotel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mkbeh/pacecache"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type statsProviderStub struct {
	name  string
	stats pacecache.Stats
}

func (provider *statsProviderStub) Name() string {
	if provider == nil {
		return ""
	}

	return provider.name
}

func (provider *statsProviderStub) Stats() pacecache.Stats {
	if provider == nil {
		return pacecache.Stats{}
	}

	return provider.stats
}

func TestRegisterCacheValidation(t *testing.T) {
	provider := &statsProviderStub{name: "users"}

	t.Run("nil metrics", func(t *testing.T) {
		var metrics *Metrics

		registration, err := metrics.RegisterCache(provider)
		if registration != nil {
			t.Fatalf("RegisterCache() registration = %v, want nil", registration)
		}
		if err == nil || err.Error() != "paceotel: metrics is nil" {
			t.Fatalf("RegisterCache() error = %v, want metrics is nil", err)
		}
	})

	t.Run("nil cache", func(t *testing.T) {
		metrics := New()

		registration, err := metrics.RegisterCache(nil)
		if registration != nil {
			t.Fatalf("RegisterCache() registration = %v, want nil", registration)
		}
		if err == nil || err.Error() != "paceotel: cache is nil" {
			t.Fatalf("RegisterCache() error = %v, want cache is nil", err)
		}
	})

	t.Run("blank cache name", func(t *testing.T) {
		metrics := New()

		registration, err := metrics.RegisterCache(&statsProviderStub{})
		if registration != nil {
			t.Fatalf("RegisterCache() registration = %v, want nil", registration)
		}
		if err == nil || err.Error() != "paceotel: cache name is empty" {
			t.Fatalf("RegisterCache() error = %v, want cache name is empty", err)
		}
	})
}

func TestRegisterCacheCollectsMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
	)
	t.Cleanup(func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	})

	stats := pacecache.Stats{
		EntryCount:   3,
		MaxEntries:   128,
		SegmentCount: 4,

		HitCount:  11,
		MissCount: 7,

		LoadFoundCount:      5,
		LoadNotFoundCount:   3,
		LoadErrorCount:      2,
		LoadSupersededCount: 1,
		LoadDuration:        1500 * time.Millisecond,
		SharedCount:         4,

		InvalidatedKeyCount: 8,
		InvalidatedAllCount: 9,

		CleanupCount:              10,
		CleanupWorkerRunCount:     11,
		CleanupWorkerPendingCount: 12,
		CleanupWorkerDuration:     2500 * time.Millisecond,

		EvictionCount:   13,
		ExpirationCount: 14,
	}

	metrics := New(
		WithMeterProvider(meterProvider),
	)

	registration, err := metrics.RegisterCache(
		&statsProviderStub{
			name:  "users",
			stats: stats,
		},
	)
	if err != nil {
		t.Fatalf("RegisterCache() error = %v", err)
	}
	if registration == nil {
		t.Fatal("RegisterCache() registration = nil")
	}
	t.Cleanup(registration.Close)

	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(
		context.Background(),
		&resourceMetrics,
	); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	collected := collectMetricsByName(t, resourceMetrics)

	requireInt64MetricPoint(
		t,
		collected,
		entryCountMetricName,
		map[string]string{cacheNameAttribute: "users"},
		3,
	)
	requireInt64MetricPoint(
		t,
		collected,
		entryLimitMetricName,
		map[string]string{cacheNameAttribute: "users"},
		128,
	)
	requireInt64MetricPoint(
		t,
		collected,
		segmentCountMetricName,
		map[string]string{cacheNameAttribute: "users"},
		4,
	)

	requireInt64MetricPoint(
		t,
		collected,
		lookupCountMetricName,
		map[string]string{
			cacheNameAttribute:    "users",
			lookupResultAttribute: lookupResultHit,
		},
		11,
	)
	requireInt64MetricPoint(
		t,
		collected,
		lookupCountMetricName,
		map[string]string{
			cacheNameAttribute:    "users",
			lookupResultAttribute: lookupResultMiss,
		},
		7,
	)
	requireNoInt64MetricPoint(
		t,
		collected,
		lookupCountMetricName,
		map[string]string{
			cacheNameAttribute:    "users",
			lookupResultAttribute: "negative_hit",
		},
	)

	requireInt64MetricPoint(
		t,
		collected,
		loadCountMetricName,
		map[string]string{
			cacheNameAttribute:  "users",
			loadResultAttribute: loadResultFound,
		},
		5,
	)
	requireInt64MetricPoint(
		t,
		collected,
		loadCountMetricName,
		map[string]string{
			cacheNameAttribute:  "users",
			loadResultAttribute: loadResultNotFound,
		},
		3,
	)
	requireInt64MetricPoint(
		t,
		collected,
		loadCountMetricName,
		map[string]string{
			cacheNameAttribute:  "users",
			loadResultAttribute: loadResultError,
		},
		2,
	)

	requireFloat64MetricPoint(
		t,
		collected,
		loadTimeMetricName,
		map[string]string{cacheNameAttribute: "users"},
		1.5,
	)
	requireInt64MetricPoint(
		t,
		collected,
		loadSharedCountMetricName,
		map[string]string{cacheNameAttribute: "users"},
		4,
	)
	requireInt64MetricPoint(
		t,
		collected,
		loadSupersededCountMetricName,
		map[string]string{cacheNameAttribute: "users"},
		1,
	)

	requireInt64MetricPoint(
		t,
		collected,
		invalidationCountMetricName,
		map[string]string{
			cacheNameAttribute:         "users",
			invalidationScopeAttribute: invalidationScopeKeys,
		},
		8,
	)
	requireInt64MetricPoint(
		t,
		collected,
		invalidationCountMetricName,
		map[string]string{
			cacheNameAttribute:         "users",
			invalidationScopeAttribute: invalidationScopeAll,
		},
		9,
	)

	requireInt64MetricPoint(
		t,
		collected,
		cleanupCountMetricName,
		map[string]string{cacheNameAttribute: "users"},
		10,
	)
	requireInt64MetricPoint(
		t,
		collected,
		cleanupWorkerRunCountMetricName,
		map[string]string{cacheNameAttribute: "users"},
		11,
	)
	requireInt64MetricPoint(
		t,
		collected,
		cleanupWorkerPendingCountMetricName,
		map[string]string{cacheNameAttribute: "users"},
		12,
	)
	requireFloat64MetricPoint(
		t,
		collected,
		cleanupWorkerTimeMetricName,
		map[string]string{cacheNameAttribute: "users"},
		2.5,
	)
	requireInt64MetricPoint(
		t,
		collected,
		evictionCountMetricName,
		map[string]string{cacheNameAttribute: "users"},
		13,
	)
	requireInt64MetricPoint(
		t,
		collected,
		expirationCountMetricName,
		map[string]string{cacheNameAttribute: "users"},
		14,
	)
}

func TestNewMetricError(t *testing.T) {
	sentinel := errors.New("instrument failed")

	err := newMetricError("pacecache.test", sentinel)
	if !errors.Is(err, sentinel) {
		t.Fatalf("newMetricError() = %v, want wrapped sentinel", err)
	}

	const want = "paceotel: create pacecache.test: instrument failed"
	if err.Error() != want {
		t.Fatalf("newMetricError() = %q, want %q", err, want)
	}
}

func collectMetricsByName(
	t *testing.T,
	resourceMetrics metricdata.ResourceMetrics,
) map[string]metricdata.Metrics {
	t.Helper()

	collected := make(map[string]metricdata.Metrics)

	var foundScope bool
	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		if scopeMetrics.Scope.Name != instrumentationName {
			continue
		}

		foundScope = true
		for _, current := range scopeMetrics.Metrics {
			collected[current.Name] = current
		}
	}

	if !foundScope {
		t.Fatalf(
			"instrumentation scope %q not found",
			instrumentationName,
		)
	}

	expected := []string{
		entryCountMetricName,
		entryLimitMetricName,
		segmentCountMetricName,
		lookupCountMetricName,
		loadCountMetricName,
		loadTimeMetricName,
		loadSharedCountMetricName,
		loadSupersededCountMetricName,
		invalidationCountMetricName,
		cleanupCountMetricName,
		cleanupWorkerRunCountMetricName,
		cleanupWorkerPendingCountMetricName,
		cleanupWorkerTimeMetricName,
		evictionCountMetricName,
		expirationCountMetricName,
	}
	for _, name := range expected {
		if _, ok := collected[name]; !ok {
			t.Fatalf("metric %q not collected", name)
		}
	}

	return collected
}

func requireInt64MetricPoint(
	t *testing.T,
	collected map[string]metricdata.Metrics,
	name string,
	attributes map[string]string,
	want int64,
) {
	t.Helper()

	points := int64MetricPoints(t, collected[name])
	for _, point := range points {
		if !metricAttributesMatch(point.Attributes, attributes) {
			continue
		}

		if point.Value != want {
			t.Fatalf(
				"metric %q value = %d, want %d; attributes=%v",
				name,
				point.Value,
				want,
				attributes,
			)
		}

		return
	}

	t.Fatalf(
		"metric %q point with attributes %v not found",
		name,
		attributes,
	)
}

func requireNoInt64MetricPoint(
	t *testing.T,
	collected map[string]metricdata.Metrics,
	name string,
	attributes map[string]string,
) {
	t.Helper()

	points := int64MetricPoints(t, collected[name])
	for _, point := range points {
		if metricAttributesMatch(point.Attributes, attributes) {
			t.Fatalf(
				"metric %q unexpectedly contains attributes %v",
				name,
				attributes,
			)
		}
	}
}

func requireFloat64MetricPoint(
	t *testing.T,
	collected map[string]metricdata.Metrics,
	name string,
	attributes map[string]string,
	want float64,
) {
	t.Helper()

	points := float64MetricPoints(t, collected[name])
	for _, point := range points {
		if !metricAttributesMatch(point.Attributes, attributes) {
			continue
		}

		if point.Value != want {
			t.Fatalf(
				"metric %q value = %v, want %v; attributes=%v",
				name,
				point.Value,
				want,
				attributes,
			)
		}

		return
	}

	t.Fatalf(
		"metric %q point with attributes %v not found",
		name,
		attributes,
	)
}

func int64MetricPoints(
	t *testing.T,
	current metricdata.Metrics,
) []metricdata.DataPoint[int64] {
	t.Helper()

	switch data := current.Data.(type) {
	case metricdata.Gauge[int64]:
		return data.DataPoints
	case metricdata.Sum[int64]:
		return data.DataPoints
	default:
		t.Fatalf(
			"metric %q data type = %T, want int64 gauge or sum",
			current.Name,
			current.Data,
		)
		return nil
	}
}

func float64MetricPoints(
	t *testing.T,
	current metricdata.Metrics,
) []metricdata.DataPoint[float64] {
	t.Helper()

	switch data := current.Data.(type) {
	case metricdata.Gauge[float64]:
		return data.DataPoints
	case metricdata.Sum[float64]:
		return data.DataPoints
	default:
		t.Fatalf(
			"metric %q data type = %T, want float64 gauge or sum",
			current.Name,
			current.Data,
		)
		return nil
	}
}

func metricAttributesMatch(
	set attribute.Set,
	want map[string]string,
) bool {
	for key, value := range want {
		current, ok := set.Value(attribute.Key(key))
		if !ok || current.AsString() != value {
			return false
		}
	}

	return true
}
