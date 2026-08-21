package paceotel

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/mkbeh/pacecache"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	entryCountMetricName                = "pacecache.entry.count"
	entryLimitMetricName                = "pacecache.entry.limit"
	segmentCountMetricName              = "pacecache.segment.count"
	lookupCountMetricName               = "pacecache.lookup.count"
	loadCountMetricName                 = "pacecache.load.count"
	loadTimeMetricName                  = "pacecache.load.time"
	loadSharedCountMetricName           = "pacecache.load.shared.count"
	loadSupersededCountMetricName       = "pacecache.load.superseded.count"
	invalidationCountMetricName         = "pacecache.entry.invalidation.count"
	cleanupCountMetricName              = "pacecache.cleanup.count"
	cleanupWorkerRunCountMetricName     = "pacecache.cleanup.worker.run.count"
	cleanupWorkerPendingCountMetricName = "pacecache.cleanup.worker.pending.count"
	cleanupWorkerTimeMetricName         = "pacecache.cleanup.worker.time"
	evictionCountMetricName             = "pacecache.entry.eviction.count"
	expirationCountMetricName           = "pacecache.entry.expiration.count"
)

const (
	cacheNameAttribute         = "pacecache.name"
	lookupResultAttribute      = "pacecache.lookup.result"
	loadResultAttribute        = "pacecache.load.result"
	invalidationScopeAttribute = "pacecache.invalidation.scope"
)

const (
	lookupResultHit  = "hit"
	lookupResultMiss = "miss"

	loadResultFound    = "found"
	loadResultNotFound = "not_found"
	loadResultError    = "error"

	invalidationScopeKeys = "keys"
	invalidationScopeAll  = "all"
)

type cacheMetricInstruments struct {
	entryCount                metric.Int64ObservableGauge
	entryLimit                metric.Int64ObservableGauge
	segmentCount              metric.Int64ObservableGauge
	lookupCount               metric.Int64ObservableCounter
	loadCount                 metric.Int64ObservableCounter
	loadTime                  metric.Float64ObservableCounter
	loadSharedCount           metric.Int64ObservableCounter
	loadSupersededCount       metric.Int64ObservableCounter
	invalidationCount         metric.Int64ObservableCounter
	cleanupCount              metric.Int64ObservableCounter
	cleanupWorkerRunCount     metric.Int64ObservableCounter
	cleanupWorkerPendingCount metric.Int64ObservableCounter
	cleanupWorkerTime         metric.Float64ObservableCounter
	evictionCount             metric.Int64ObservableCounter
	expirationCount           metric.Int64ObservableCounter
}

type cacheMetricAttributes struct {
	base metric.ObserveOption

	hit  metric.ObserveOption
	miss metric.ObserveOption

	loadFound    metric.ObserveOption
	loadNotFound metric.ObserveOption
	loadError    metric.ObserveOption

	invalidateKeys metric.ObserveOption
	invalidateAll  metric.ObserveOption
}

// RegisterCache registers OpenTelemetry metrics for one cache.
func (metrics *Metrics) RegisterCache(
	cache pacecache.StatsProvider,
) (pacecache.MetricsRegistration, error) {
	if metrics == nil {
		return nil, errors.New("paceotel: metrics is nil")
	}

	if cache == nil {
		return nil, errors.New("paceotel: cache is nil")
	}

	name := cache.Name()
	if name == "" {
		return nil, errors.New("paceotel: cache name is empty")
	}

	meterProvider := metrics.meterProvider
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}

	return registerCacheMetrics(cache, name, meterProvider)
}

func registerCacheMetrics(
	cache pacecache.StatsProvider,
	name string,
	provider metric.MeterProvider,
) (pacecache.MetricsRegistration, error) {
	meter := provider.Meter(instrumentationName)

	instruments, err := newCacheMetricInstruments(meter)
	if err != nil {
		return nil, err
	}

	attributes := newCacheMetricAttributes(name)

	registration, err := meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			instruments.observe(observer, cache.Stats(), attributes)

			return nil
		},
		instruments.observables()...,
	)
	if err != nil {
		return nil, fmt.Errorf("paceotel: register metrics callback: %w", err)
	}

	return &metricsRegistration{
		registration: registration,
	}, nil
}

func (instruments cacheMetricInstruments) observe(
	observer metric.Observer,
	stats pacecache.Stats,
	attributes cacheMetricAttributes,
) {
	observer.ObserveInt64(
		instruments.entryCount,
		stats.EntryCount,
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.entryLimit,
		stats.MaxEntries,
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.segmentCount,
		stats.SegmentCount,
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.lookupCount,
		stats.HitCount,
		attributes.hit,
	)

	observer.ObserveInt64(
		instruments.lookupCount,
		stats.MissCount,
		attributes.miss,
	)

	observer.ObserveInt64(
		instruments.loadCount,
		stats.LoadFoundCount,
		attributes.loadFound,
	)

	observer.ObserveInt64(
		instruments.loadCount,
		stats.LoadNotFoundCount,
		attributes.loadNotFound,
	)

	observer.ObserveInt64(
		instruments.loadCount,
		stats.LoadErrorCount,
		attributes.loadError,
	)

	observer.ObserveFloat64(
		instruments.loadTime,
		stats.LoadDuration.Seconds(),
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.loadSharedCount,
		stats.SharedCount,
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.loadSupersededCount,
		stats.LoadSupersededCount,
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.invalidationCount,
		stats.InvalidatedKeyCount,
		attributes.invalidateKeys,
	)

	observer.ObserveInt64(
		instruments.invalidationCount,
		stats.InvalidatedAllCount,
		attributes.invalidateAll,
	)

	observer.ObserveInt64(
		instruments.cleanupCount,
		stats.CleanupCount,
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.cleanupWorkerRunCount,
		stats.CleanupWorkerRunCount,
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.cleanupWorkerPendingCount,
		stats.CleanupWorkerPendingCount,
		attributes.base,
	)

	observer.ObserveFloat64(
		instruments.cleanupWorkerTime,
		stats.CleanupWorkerDuration.Seconds(),
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.evictionCount,
		stats.EvictionCount,
		attributes.base,
	)

	observer.ObserveInt64(
		instruments.expirationCount,
		stats.ExpirationCount,
		attributes.base,
	)
}

func (instruments cacheMetricInstruments) observables() []metric.Observable {
	return []metric.Observable{
		instruments.entryCount,
		instruments.entryLimit,
		instruments.segmentCount,
		instruments.lookupCount,
		instruments.loadCount,
		instruments.loadTime,
		instruments.loadSharedCount,
		instruments.loadSupersededCount,
		instruments.invalidationCount,
		instruments.cleanupCount,
		instruments.cleanupWorkerRunCount,
		instruments.cleanupWorkerPendingCount,
		instruments.cleanupWorkerTime,
		instruments.evictionCount,
		instruments.expirationCount,
	}
}

func newCacheMetricInstruments(
	meter metric.Meter,
) (cacheMetricInstruments, error) {
	var instruments cacheMetricInstruments
	var err error

	instruments.entryCount, err = meter.Int64ObservableGauge(
		entryCountMetricName,
		metric.WithDescription(
			"The number of entries currently resident in cache storage.",
		),
		metric.WithUnit("{entry}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(entryCountMetricName, err)
	}

	instruments.entryLimit, err = meter.Int64ObservableGauge(
		entryLimitMetricName,
		metric.WithDescription(
			"The configured total cache entry budget.",
		),
		metric.WithUnit("{entry}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(entryLimitMetricName, err)
	}

	instruments.segmentCount, err = meter.Int64ObservableGauge(
		segmentCountMetricName,
		metric.WithDescription(
			"The number of independent cache storage segments.",
		),
		metric.WithUnit("{segment}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(segmentCountMetricName, err)
	}

	instruments.lookupCount, err = meter.Int64ObservableCounter(
		lookupCountMetricName,
		metric.WithDescription(
			"The cumulative number of cache lookups by result.",
		),
		metric.WithUnit("{lookup}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(lookupCountMetricName, err)
	}

	instruments.loadCount, err = meter.Int64ObservableCounter(
		loadCountMetricName,
		metric.WithDescription(
			"The cumulative number of actual cache loader invocations by result.",
		),
		metric.WithUnit("{load}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(loadCountMetricName, err)
	}

	instruments.loadTime, err = meter.Float64ObservableCounter(
		loadTimeMetricName,
		metric.WithDescription(
			"The cumulative time spent in actual cache loader invocations.",
		),
		metric.WithUnit("s"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(loadTimeMetricName, err)
	}

	instruments.loadSharedCount, err = meter.Int64ObservableCounter(
		loadSharedCountMetricName,
		metric.WithDescription(
			"The cumulative number of cache requests that received a shared load result.",
		),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(loadSharedCountMetricName, err)
	}

	instruments.loadSupersededCount, err = meter.Int64ObservableCounter(
		loadSupersededCountMetricName,
		metric.WithDescription(
			"The cumulative number of successful loader invocations discarded because a newer cache mutation won before publication.",
		),
		metric.WithUnit("{load}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(loadSupersededCountMetricName, err)
	}

	instruments.invalidationCount, err = meter.Int64ObservableCounter(
		invalidationCountMetricName,
		metric.WithDescription(
			"The cumulative number of resident cache entries removed by explicit invalidation, by scope.",
		),
		metric.WithUnit("{entry}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(invalidationCountMetricName, err)
	}

	instruments.cleanupCount, err = meter.Int64ObservableCounter(
		cleanupCountMetricName,
		metric.WithDescription(
			"The cumulative number of completed explicit cache cleanup operations.",
		),
		metric.WithUnit("{cleanup}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(cleanupCountMetricName, err)
	}

	instruments.cleanupWorkerRunCount, err = meter.Int64ObservableCounter(
		cleanupWorkerRunCountMetricName,
		metric.WithDescription(
			"The cumulative number of cleanup quanta completed by the background cleanup worker.",
		),
		metric.WithUnit("{run}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(cleanupWorkerRunCountMetricName, err)
	}

	instruments.cleanupWorkerPendingCount, err = meter.Int64ObservableCounter(
		cleanupWorkerPendingCountMetricName,
		metric.WithDescription(
			"The cumulative number of background cleanup quanta that completed with expired entries still pending.",
		),
		metric.WithUnit("{run}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(cleanupWorkerPendingCountMetricName, err)
	}

	instruments.cleanupWorkerTime, err = meter.Float64ObservableCounter(
		cleanupWorkerTimeMetricName,
		metric.WithDescription(
			"The cumulative time spent executing background cleanup quanta.",
		),
		metric.WithUnit("s"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(cleanupWorkerTimeMetricName, err)
	}

	instruments.evictionCount, err = meter.Int64ObservableCounter(
		evictionCountMetricName,
		metric.WithDescription(
			"The cumulative number of cache entries evicted because a storage segment reached capacity.",
		),
		metric.WithUnit("{entry}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(evictionCountMetricName, err)
	}

	instruments.expirationCount, err = meter.Int64ObservableCounter(
		expirationCountMetricName,
		metric.WithDescription(
			"The cumulative number of expired cache entries removed from storage.",
		),
		metric.WithUnit("{entry}"),
	)
	if err != nil {
		return cacheMetricInstruments{},
			newMetricError(expirationCountMetricName, err)
	}

	return instruments, nil
}

func newCacheMetricAttributes(name string) cacheMetricAttributes {
	base := []attribute.KeyValue{
		attribute.String(
			cacheNameAttribute,
			name,
		),
	}

	option := func(extra ...attribute.KeyValue) metric.ObserveOption {
		return metric.WithAttributeSet(
			attribute.NewSet(
				slices.Concat(base, extra)...),
		)
	}

	return cacheMetricAttributes{
		base: option(),

		hit: option(
			attribute.String(
				lookupResultAttribute,
				lookupResultHit,
			),
		),

		miss: option(
			attribute.String(
				lookupResultAttribute,
				lookupResultMiss,
			),
		),

		loadFound: option(
			attribute.String(
				loadResultAttribute,
				loadResultFound,
			),
		),

		loadNotFound: option(
			attribute.String(
				loadResultAttribute,
				loadResultNotFound,
			),
		),

		loadError: option(
			attribute.String(
				loadResultAttribute,
				loadResultError,
			),
		),

		invalidateKeys: option(
			attribute.String(
				invalidationScopeAttribute,
				invalidationScopeKeys,
			),
		),

		invalidateAll: option(
			attribute.String(
				invalidationScopeAttribute,
				invalidationScopeAll,
			),
		),
	}
}

func newMetricError(name string, err error) error {
	return fmt.Errorf("paceotel: create %s: %w", name, err)
}
