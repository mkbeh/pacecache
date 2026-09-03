package pacecache

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

const (
	// DefaultExpiration uses the cache's configured TTL.
	DefaultExpiration time.Duration = 0

	// NoExpiration disables time-based expiration for the entry.
	NoExpiration time.Duration = -1
)

// Cache is a bounded in-process cache for keys of type K and values of type V.
//
// Cache uses exact LRU eviction within each storage segment and TTL expiration.
// Entries may optionally use sliding expiration. GetOrLoad, GetOrLoadWith,
// GetOrLoadEntry, and GetOrLoadEntryWith provide cache-aside loading and
// coalesce concurrent loads for the same key.
//
// Cache is safe for concurrent use. A Cache must not be copied after creation.
type Cache[K comparable, V any] struct {
	name   string
	loader Loader[K, V]

	store  *storage[K, V]
	states []cacheState[K, V]
	stats  *statsCollector

	cleanupPolicy cleanupPolicy
	cleanup       *cleanupWorker[K, V]

	metrics   MetricsRegistration
	closeOnce sync.Once

	ttl    time.Duration
	jitter time.Duration
}

// New creates a Cache with the given logical name.
//
// Name is used by diagnostics and metrics and must not be empty.
//
// Unless overridden by options, New uses the default cache capacity, a single
// storage segment, and no time-based expiration. No default loader is
// configured. Metrics and background cleanup are disabled by default.
//
// If metrics or background cleanup are configured, Close must be called to
// release the associated resources.
func New[K comparable, V any](name string, options ...Option) (*Cache[K, V], error) {
	return newCache[K, V](name, nil, options...)
}

// NewWithLoader creates a Cache with the given logical name and default loader.
//
// The loader is used by GetOrLoad and GetOrLoadEntry when no live cache entry
// exists. Per-call loaders may be supplied through GetOrLoadWith and
// GetOrLoadEntryWith. Loader must not be nil.
//
// Name, options, metrics, and background cleanup have the same semantics as New.
func NewWithLoader[K comparable, V any](name string, loader Loader[K, V], options ...Option) (*Cache[K, V], error) {
	if loader == nil {
		return nil, ErrNoLoader
	}

	return newCache[K, V](name, loader, options...)
}

func newCache[K comparable, V any](
	name string,
	loader Loader[K, V],
	options ...Option,
) (*Cache[K, V], error) {
	settings, err := newCacheSettings(name, options...)
	if err != nil {
		return nil, fmt.Errorf("pacecache: %w", err)
	}

	store := newStorage[K, V](
		settings.maxEntries,
		settings.segmentCount,
		settings.slidingExpiration,
	)

	policy := cleanupPolicy{
		batchSize:   settings.cleanupBatchSize,
		entryBudget: settings.cleanupEntryBudget,
	}

	cache := &Cache[K, V]{
		name:   settings.name,
		loader: loader,

		store:  store,
		states: make([]cacheState[K, V], len(store.segments)),
		stats:  newStatsCollector(len(store.segments)),

		cleanupPolicy: policy,

		ttl:    settings.ttl,
		jitter: settings.jitter,
	}

	if err := cache.registerMetrics(settings.metrics); err != nil {
		return nil, fmt.Errorf("pacecache: register metrics: %w", err)
	}

	if settings.cleanupInterval > 0 {
		cache.cleanup = newCleanupWorker(
			cache.store,
			cache.stats,
			cache.cleanupPolicy,
			settings.cleanupInterval,
		)
		cache.cleanup.start()
	}

	return cache, nil
}

// Name returns the logical cache name.
func (cache *Cache[K, V]) Name() string {
	if cache == nil {
		return ""
	}

	return cache.name
}

// Close releases background resources associated with the cache. Close is
// idempotent.
//
// If background cleanup is configured, Close stops it and waits for the cleaner
// goroutine to exit. If metrics are configured, Close also releases their
// registration.
//
// Close does not clear or disable the cache. Cache operations remain available,
// but stopped background resources are not restarted.
func (cache *Cache[K, V]) Close() {
	if cache == nil {
		return
	}

	cache.closeOnce.Do(func() {
		if cache.cleanup != nil {
			cache.cleanup.close()
		}

		if cache.metrics != nil {
			cache.metrics.Close()
		}
	})
}

func (cache *Cache[K, V]) effectiveTTL(expiration time.Duration) time.Duration {
	ttl := expiration

	if ttl == DefaultExpiration {
		ttl = cache.ttl
	}

	if ttl <= 0 {
		return 0
	}

	return jitteredTTL(ttl, cache.jitter)
}

func (cache *Cache[K, V]) registerMetrics(metrics Metrics) error {
	if metrics == nil {
		return nil
	}

	registration, err := metrics.RegisterCache(
		cacheStatsProvider[K, V]{
			cache: cache,
		},
	)
	if err != nil {
		return err
	}

	cache.metrics = registration

	return nil
}

func (cache *Cache[K, V]) initialized() bool {
	return cache != nil &&
		cache.store != nil &&
		cache.stats != nil
}

func deadlineAfter(now int64, ttl time.Duration) int64 {
	delta := int64(ttl)
	if delta <= 0 {
		return 0
	}

	maxDeadline := int64(maxDuration)
	if now >= maxDeadline-delta {
		return maxDeadline
	}

	return now + delta
}

func jitteredTTL(ttl, jitter time.Duration) time.Duration {
	if jitter == 0 {
		return ttl
	}

	jitterLimit := min(jitter, maxDuration-ttl)
	if jitterLimit <= 0 {
		return ttl
	}

	return ttl + time.Duration(
		rand.Int64N(int64(jitterLimit)),
	)
}
