package paceotel

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mkbeh/pacecache"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	wantScopeName = "github.com/mkbeh/pacecache/extra/paceotel"

	wantEntryCountMetricName                = "pacecache.entry.count"
	wantEntryLimitMetricName                = "pacecache.entry.limit"
	wantSegmentCountMetricName              = "pacecache.segment.count"
	wantLookupCountMetricName               = "pacecache.lookup.count"
	wantLoadCountMetricName                 = "pacecache.load.count"
	wantLoadTimeMetricName                  = "pacecache.load.time"
	wantLoadSharedCountMetricName           = "pacecache.load.shared.count"
	wantLoadSupersededCountMetricName       = "pacecache.load.superseded.count"
	wantRemovedCountMetricName              = "pacecache.entry.removed.count"
	wantCleanupCountMetricName              = "pacecache.cleanup.count"
	wantCleanupWorkerRunCountMetricName     = "pacecache.cleanup.worker.run.count"
	wantCleanupWorkerPendingCountMetricName = "pacecache.cleanup.worker.pending.count"
	wantCleanupWorkerTimeMetricName         = "pacecache.cleanup.worker.time"
	wantEvictionCountMetricName             = "pacecache.entry.eviction.count"
	wantExpirationCountMetricName           = "pacecache.entry.expiration.count"

	wantCacheNameAttribute        = "pacecache.name"
	wantLookupResultAttribute     = "pacecache.lookup.result"
	wantLoadResultAttribute       = "pacecache.load.result"
	wantRemovalOperationAttribute = "pacecache.removal.operation"
)

func TestMetricsSchema(t *testing.T) {
	metrics, reader := newTestMetrics(t)

	if err := metrics.Register(&metricsSourceStub{name: "users"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	collected := collectMetricsByName(t, collectTestMetrics(t, reader))

	expected := map[string]string{
		wantEntryCountMetricName:                "{entry}",
		wantEntryLimitMetricName:                "{entry}",
		wantSegmentCountMetricName:              "{segment}",
		wantLookupCountMetricName:               "{lookup}",
		wantLoadCountMetricName:                 "{load}",
		wantLoadTimeMetricName:                  "s",
		wantLoadSharedCountMetricName:           "{request}",
		wantLoadSupersededCountMetricName:       "{load}",
		wantRemovedCountMetricName:              "{entry}",
		wantCleanupCountMetricName:              "{cleanup}",
		wantCleanupWorkerRunCountMetricName:     "{run}",
		wantCleanupWorkerPendingCountMetricName: "{run}",
		wantCleanupWorkerTimeMetricName:         "s",
		wantEvictionCountMetricName:             "{entry}",
		wantExpirationCountMetricName:           "{entry}",
	}

	if len(collected) != len(expected) {
		t.Fatalf("collected metrics = %d, want %d", len(collected), len(expected))
	}

	for name, unit := range expected {
		current := requireMetric(t, collected, name)
		if current.Unit != unit {
			t.Fatalf("metric %q unit = %q, want %q", name, current.Unit, unit)
		}
	}
}

func TestMetricsCollectsSource(t *testing.T) {
	metrics, reader := newTestMetrics(t)

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

		DeletedEntryCount: 8,
		ClearedEntryCount: 9,

		CleanupCount:              10,
		CleanupWorkerRunCount:     11,
		CleanupWorkerPendingCount: 12,
		CleanupWorkerDuration:     2500 * time.Millisecond,

		EvictionCount:   13,
		ExpirationCount: 14,
	}

	var statsCalls atomic.Int64

	if err := metrics.Register(
		&metricsSourceStub{
			name: "users",
			statsFn: func() pacecache.Stats {
				statsCalls.Add(1)

				return stats
			},
		},
	); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	collected := collectMetricsByName(t, collectTestMetrics(t, reader))

	if got := statsCalls.Load(); got != 1 {
		t.Fatalf("Stats() calls = %d, want 1", got)
	}

	requireInt64GaugePoint(
		t,
		collected,
		wantEntryCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		3,
	)
	requireInt64GaugePoint(
		t,
		collected,
		wantEntryLimitMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		128,
	)
	requireInt64GaugePoint(
		t,
		collected,
		wantSegmentCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		4,
	)

	lookupPoints := int64CounterPoints(
		t,
		requireMetric(t, collected, wantLookupCountMetricName),
	)
	if len(lookupPoints) != 2 {
		t.Fatalf("lookup count points = %d, want 2", len(lookupPoints))
	}
	requireInt64CounterPoint(
		t,
		collected,
		wantLookupCountMetricName,
		map[string]string{
			wantCacheNameAttribute:    "users",
			wantLookupResultAttribute: "hit",
		},
		11,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantLookupCountMetricName,
		map[string]string{
			wantCacheNameAttribute:    "users",
			wantLookupResultAttribute: "miss",
		},
		7,
	)

	loadPoints := int64CounterPoints(
		t,
		requireMetric(t, collected, wantLoadCountMetricName),
	)
	if len(loadPoints) != 3 {
		t.Fatalf("load count points = %d, want 3", len(loadPoints))
	}
	requireInt64CounterPoint(
		t,
		collected,
		wantLoadCountMetricName,
		map[string]string{
			wantCacheNameAttribute:  "users",
			wantLoadResultAttribute: "found",
		},
		5,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantLoadCountMetricName,
		map[string]string{
			wantCacheNameAttribute:  "users",
			wantLoadResultAttribute: "not_found",
		},
		3,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantLoadCountMetricName,
		map[string]string{
			wantCacheNameAttribute:  "users",
			wantLoadResultAttribute: "error",
		},
		2,
	)

	requireFloat64CounterPoint(
		t,
		collected,
		wantLoadTimeMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		1.5,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantLoadSharedCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		4,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantLoadSupersededCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		1,
	)

	removedPoints := int64CounterPoints(
		t,
		requireMetric(t, collected, wantRemovedCountMetricName),
	)
	if len(removedPoints) != 2 {
		t.Fatalf("removed count points = %d, want 2", len(removedPoints))
	}
	requireInt64CounterPoint(
		t,
		collected,
		wantRemovedCountMetricName,
		map[string]string{
			wantCacheNameAttribute:        "users",
			wantRemovalOperationAttribute: "delete",
		},
		8,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantRemovedCountMetricName,
		map[string]string{
			wantCacheNameAttribute:        "users",
			wantRemovalOperationAttribute: "clear",
		},
		9,
	)

	requireInt64CounterPoint(
		t,
		collected,
		wantCleanupCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		10,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantCleanupWorkerRunCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		11,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantCleanupWorkerPendingCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		12,
	)
	requireFloat64CounterPoint(
		t,
		collected,
		wantCleanupWorkerTimeMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		2.5,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantEvictionCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		13,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantExpirationCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		14,
	)
}

func TestMetricsCollectsMultipleSources(t *testing.T) {
	metrics, reader := newTestMetrics(t)

	if err := metrics.Register(
		&metricsSourceStub{
			name: "users",
			stats: pacecache.Stats{
				EntryCount: 3,
				MaxEntries: 128,
				HitCount:   11,
			},
		},
	); err != nil {
		t.Fatalf("Register(users) error = %v", err)
	}

	if err := metrics.Register(
		&metricsSourceStub{
			name: "sessions",
			stats: pacecache.Stats{
				EntryCount: 7,
				MaxEntries: 64,
				MissCount:  5,
			},
		},
	); err != nil {
		t.Fatalf("Register(sessions) error = %v", err)
	}

	collected := collectMetricsByName(t, collectTestMetrics(t, reader))

	entryPoints := int64GaugePoints(
		t,
		requireMetric(t, collected, wantEntryCountMetricName),
	)
	if len(entryPoints) != 2 {
		t.Fatalf("entry count points = %d, want 2", len(entryPoints))
	}

	requireInt64GaugePoint(
		t,
		collected,
		wantEntryCountMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		3,
	)
	requireInt64GaugePoint(
		t,
		collected,
		wantEntryCountMetricName,
		map[string]string{wantCacheNameAttribute: "sessions"},
		7,
	)
	requireInt64GaugePoint(
		t,
		collected,
		wantEntryLimitMetricName,
		map[string]string{wantCacheNameAttribute: "users"},
		128,
	)
	requireInt64GaugePoint(
		t,
		collected,
		wantEntryLimitMetricName,
		map[string]string{wantCacheNameAttribute: "sessions"},
		64,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantLookupCountMetricName,
		map[string]string{
			wantCacheNameAttribute:    "users",
			wantLookupResultAttribute: "hit",
		},
		11,
	)
	requireInt64CounterPoint(
		t,
		collected,
		wantLookupCountMetricName,
		map[string]string{
			wantCacheNameAttribute:    "sessions",
			wantLookupResultAttribute: "miss",
		},
		5,
	)
}

func TestMetricsCollectsUnnamedSource(t *testing.T) {
	metrics, reader := newTestMetrics(t)

	if err := metrics.Register(
		&metricsSourceStub{
			stats: pacecache.Stats{EntryCount: 3},
		},
	); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	collected := collectMetricsByName(t, collectTestMetrics(t, reader))
	points := int64GaugePoints(
		t,
		requireMetric(t, collected, wantEntryCountMetricName),
	)

	if len(points) != 1 {
		t.Fatalf("entry count points = %d, want 1", len(points))
	}
	if points[0].Value != 3 {
		t.Fatalf("entry count = %d, want 3", points[0].Value)
	}
	if got := points[0].Attributes.Len(); got != 0 {
		t.Fatalf("unnamed source attributes = %d, want 0", got)
	}
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

func collectTestMetrics(
	t *testing.T,
	reader *sdkmetric.ManualReader,
) metricdata.ResourceMetrics {
	t.Helper()

	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resourceMetrics); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	return resourceMetrics
}

func collectMetricsByName(
	t *testing.T,
	resourceMetrics metricdata.ResourceMetrics,
) map[string]metricdata.Metrics {
	t.Helper()

	collected := make(map[string]metricdata.Metrics)
	foundScope := false

	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		if scopeMetrics.Scope.Name != wantScopeName {
			continue
		}

		if foundScope {
			t.Fatalf("instrumentation scope %q collected more than once", wantScopeName)
		}
		foundScope = true

		for _, current := range scopeMetrics.Metrics {
			if _, exists := collected[current.Name]; exists {
				t.Fatalf("metric %q collected more than once", current.Name)
			}

			collected[current.Name] = current
		}
	}

	if !foundScope {
		t.Fatalf("instrumentation scope %q not found", wantScopeName)
	}

	return collected
}

func requireMetric(
	t *testing.T,
	collected map[string]metricdata.Metrics,
	name string,
) metricdata.Metrics {
	t.Helper()

	current, ok := collected[name]
	if !ok {
		t.Fatalf("metric %q not collected", name)
	}

	return current
}

func requireInt64GaugePoint(
	t *testing.T,
	collected map[string]metricdata.Metrics,
	name string,
	attributes map[string]string,
	want int64,
) {
	t.Helper()

	points := int64GaugePoints(t, requireMetric(t, collected, name))
	requireInt64Point(t, name, points, attributes, want)
}

func requireInt64CounterPoint(
	t *testing.T,
	collected map[string]metricdata.Metrics,
	name string,
	attributes map[string]string,
	want int64,
) {
	t.Helper()

	points := int64CounterPoints(t, requireMetric(t, collected, name))
	requireInt64Point(t, name, points, attributes, want)
}

func requireFloat64CounterPoint(
	t *testing.T,
	collected map[string]metricdata.Metrics,
	name string,
	attributes map[string]string,
	want float64,
) {
	t.Helper()

	points := float64CounterPoints(t, requireMetric(t, collected, name))
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

func requireInt64Point(
	t *testing.T,
	name string,
	points []metricdata.DataPoint[int64],
	attributes map[string]string,
	want int64,
) {
	t.Helper()

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

func int64GaugePoints(
	t *testing.T,
	current metricdata.Metrics,
) []metricdata.DataPoint[int64] {
	t.Helper()

	data, ok := current.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf(
			"metric %q data type = %T, want int64 gauge",
			current.Name,
			current.Data,
		)
	}

	return data.DataPoints
}

func int64CounterPoints(
	t *testing.T,
	current metricdata.Metrics,
) []metricdata.DataPoint[int64] {
	t.Helper()

	data, ok := current.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf(
			"metric %q data type = %T, want int64 sum",
			current.Name,
			current.Data,
		)
	}
	if !data.IsMonotonic {
		t.Fatalf("metric %q is not monotonic", current.Name)
	}

	return data.DataPoints
}

func float64CounterPoints(
	t *testing.T,
	current metricdata.Metrics,
) []metricdata.DataPoint[float64] {
	t.Helper()

	data, ok := current.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf(
			"metric %q data type = %T, want float64 sum",
			current.Name,
			current.Data,
		)
	}
	if !data.IsMonotonic {
		t.Fatalf("metric %q is not monotonic", current.Name)
	}

	return data.DataPoints
}

func hasMetricAttributes(
	points []metricdata.DataPoint[int64],
	want map[string]string,
) bool {
	for _, point := range points {
		if metricAttributesMatch(point.Attributes, want) {
			return true
		}
	}

	return false
}

func metricAttributesMatch(
	set attribute.Set,
	want map[string]string,
) bool {
	if set.Len() != len(want) {
		return false
	}

	for key, value := range want {
		current, ok := set.Value(attribute.Key(key))
		if !ok || current.AsString() != value {
			return false
		}
	}

	return true
}
