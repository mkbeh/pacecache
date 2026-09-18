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
// Entries may optionally use sliding expiration. GetOrLoad, GetOrLoadFunc,
// GetOrLoadEntry, and GetOrLoadEntryFunc provide cache-aside loading and
// coalesce concurrent loads for the same key.
//
// Cache is safe for concurrent use. A Cache must not be copied after creation.
type Cache[K comparable, V any] struct {
	loader Loader[K, V]

	store  *storage[K, V]
	states []cacheState[K, V]
	stats  *statsCollector

	cleanupPolicy cleanupPolicy
	cleanup       *cleanupWorker[K, V]
	closeOnce     sync.Once

	ttl    time.Duration
	jitter time.Duration
}

// New creates a Cache.
//
// Unless overridden by options, New uses the default cache capacity, a single
// storage segment, and no time-based expiration. No default loader is
// configured. Metrics and background cleanup are disabled by default.
func New[K comparable, V any](
	options ...Option,
) (*Cache[K, V], error) {
	return newCache[K, V](nil, options...)
}

// NewWithDefaultLoader creates a Cache with the given default loader.
//
// The loader is used by GetOrLoad and GetOrLoadEntry when no live cache entry
// exists. Per-call loaders may be supplied through GetOrLoadFunc and
// GetOrLoadEntryFunc. Loader must not be nil.
//
// Options, metrics, and background cleanup have the same semantics as New.
func NewWithDefaultLoader[K comparable, V any](
	loader Loader[K, V],
	options ...Option,
) (*Cache[K, V], error) {
	if loader == nil {
		return nil, ErrNoLoader
	}

	return newCache[K, V](loader, options...)
}

func newCache[K comparable, V any](
	loader Loader[K, V],
	options ...Option,
) (*Cache[K, V], error) {
	settings, err := newSettings(options...)
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
		loader: loader,

		store:  store,
		states: make([]cacheState[K, V], len(store.segments)),
		stats:  newStatsCollector(len(store.segments)),

		cleanupPolicy: policy,

		ttl:    settings.ttl,
		jitter: settings.jitter,
	}

	if err := cache.registerMetrics(settings.name, settings.metrics); err != nil {
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

// Close stops background cleanup and waits for the worker to exit.
//
// Close does not clear or disable the cache. Repeated calls are safe. Close is
// a no-op on a nil Cache.
func (cache *Cache[K, V]) Close() {
	if cache == nil {
		return
	}

	cache.closeOnce.Do(func() {
		if cache.cleanup != nil {
			cache.cleanup.close()
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

func (cache *Cache[K, V]) registerMetrics(name string, metrics Metrics) error {
	if metrics == nil {
		return nil
	}

	return metrics.Register(
		metricsSource[K, V]{
			name:  name,
			cache: cache,
		},
	)
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
