package pacecache

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// DefaultExpiration uses the cache's configured positive TTL.
	DefaultExpiration time.Duration = 0

	// NoExpiration disables time-based expiration for the entry.
	NoExpiration time.Duration = -1
)

// Cache is a bounded in-process read-through cache for values of type V.
//
// Cache uses segmented exact LRU eviction and TTL expiration. Positive entries
// may optionally use sliding expiration, while negative results always use
// absolute expiration. Concurrent loads for the same key are coalesced through
// singleflight.
//
// Cache is safe for concurrent use. A Cache must not be copied after creation.
type Cache[V any] struct {
	name string

	store  *storage[V]
	states []cacheState
	stats  *statsCollector

	cleanup *cleanupWorker[V]

	metrics   MetricsRegistration
	closeOnce sync.Once

	ttl         time.Duration
	jitter      time.Duration
	negativeTTL time.Duration
}

// cacheState coordinates loads and invalidation for one storage segment.
//
// mu establishes ordering between singleflight registration, cache
// publication, and invalidation. generation prevents a load registered before
// an invalidation barrier from repopulating the cache afterward.
type cacheState struct {
	mu sync.RWMutex

	generation uint64
	group      *singleflight.Group
}

type invalidationTarget struct {
	key   string
	index int
}

// New creates a Cache with the given logical name.
//
// Name is used by diagnostics and metrics and must not be blank.
//
// Unless overridden by options, New uses production-oriented defaults for
// capacity, segmentation, and positive TTL. Negative caching, metrics, and
// background cleanup are disabled by default.
//
// If metrics or background cleanup are configured, Close must be called to
// release the associated resources.
func New[V any](name string, options ...Option) (*Cache[V], error) {
	settings, err := newCacheSettings(name, options...)
	if err != nil {
		return nil, fmt.Errorf("pacecache: %w", err)
	}

	store := newStorage[V](
		settings.maxEntries,
		settings.segmentCount,
		settings.slidingExpiration,
	)

	cache := &Cache[V]{
		name: settings.name,

		store:  store,
		states: newCacheStates(len(store.segments)),
		stats:  newStatsCollector(len(store.segments)),

		ttl:         settings.ttl,
		jitter:      settings.jitter,
		negativeTTL: settings.negativeTTL,
	}

	if err := cache.registerMetrics(settings.metrics); err != nil {
		return nil, fmt.Errorf("pacecache: register metrics: %w", err)
	}

	if settings.cleanupInterval > 0 {
		cache.cleanup = newCleanupWorker(
			cache.store,
			cache.stats,
			cleanupConfig{
				interval:    settings.cleanupInterval,
				batchSize:   settings.cleanupBatchSize,
				entryBudget: settings.cleanupEntryBudget,
			},
		)
		cache.cleanup.start()
	}

	return cache, nil
}

// Name returns the logical cache name.
func (cache *Cache[V]) Name() string {
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
func (cache *Cache[V]) Close() {
	if cache == nil {
		return
	}

	cache.closeOnce.Do(
		func() {
			if cache.cleanup != nil {
				cache.cleanup.close()
			}

			if cache.metrics != nil {
				cache.metrics.Close()
			}
		},
	)
}

// Get returns the cached value for key.
//
// LookupHit is returned for a positive cache hit. LookupNegativeHit is
// returned for a cached not-found result. LookupMiss is returned when no live
// entry exists for key.
//
// Expired entries are always treated as misses. They are removed when observed,
// by CleanupExpired, or by background cleanup when it is enabled. When sliding
// expiration is enabled, a live positive hit refreshes the entry using the TTL
// with which it was stored.
func (cache *Cache[V]) Get(key string) (V, LookupStatus) {
	var zero V

	if !cache.initialized() {
		return zero, LookupMiss
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.shard(index)

	cached, ok := cache.store.lookupAt(index, key, cache.store.now(), stats)
	if !ok {
		return zero, LookupMiss
	}

	if !cached.found {
		return zero, LookupNegativeHit
	}

	return cached.value, LookupHit
}

// CleanupExpired physically removes expired entries using the cache expiration
// index and returns the number of entries removed.
//
// CleanupExpired is always available; background cleanup does not need to be
// enabled. Logical expiration is independent of physical cleanup: an expired
// entry is never returned even if it has not yet been reclaimed. Nearby
// expiration deadlines are grouped internally. Bucket eligibility may trail
// the exact TTL deadline by up to the internal bucket resolution; actual
// physical reclamation also depends on when cleanup runs.
func (cache *Cache[V]) CleanupExpired() int64 {
	if !cache.initialized() {
		return 0
	}

	return cache.store.cleanupExpired(cache.store.now(), cache.stats)
}

// GetOrLoad returns the cached result for key or obtains it from loader.
//
// A loader result with found=true is cached using the positive TTL. A result
// with found=false and a nil error represents a successful negative lookup and
// is cached only when negative caching is enabled with WithNegativeTTL.
// Loader errors are never cached.
//
// Concurrent misses for the same key are coalesced. The loader runs with the
// context of the caller that starts the shared load. Other callers may stop
// waiting independently when their own contexts are canceled.
//
// The returned found value describes whether the underlying value exists; it
// is false for both a freshly loaded and a cached negative result.
func (cache *Cache[V]) GetOrLoad(
	ctx context.Context,
	key string,
	loader Loader[V],
) (V, bool, error) {
	var zero V

	if !cache.initialized() {
		return zero, false, errors.New("pacecache: cache is not initialized")
	}

	if ctx == nil {
		return zero, false, errors.New("pacecache: context is nil")
	}

	if loader == nil {
		return zero, false, errors.New("pacecache: loader is nil")
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.shard(index)

	if cached, ok := cache.store.lookupAt(index, key, cache.store.now(), stats); ok {
		return cached.value, cached.found, nil
	}

	state := &cache.states[index]

	// Registration and generation selection must be atomic with respect to
	// invalidation for this state segment. DoChan only registers or starts the
	// shared call; the loader itself executes outside state.mu.
	state.mu.RLock()

	generation := state.generation
	group := state.group

	resultChannel := group.DoChan(
		key,
		func() (any, error) {
			// Another caller may have populated the cache between the initial
			// lookup and this call becoming the singleflight owner.
			if cached, ok := cache.store.getAt(index, key, cache.store.now(), stats); ok {
				return loadResult[V]{
					value: cached.value,
					found: cached.found,
				}, nil
			}

			startedAt := time.Now()

			value, found, err := loader(ctx)

			finishedAt := time.Now()

			cache.stats.recordLoad(index, found, err, finishedAt.Sub(startedAt))

			if err != nil {
				return nil, err
			}

			if !found {
				var zeroValue V

				value = zeroValue
			}

			loaded := loadResult[V]{
				value: value,
				found: found,
			}

			// Generation validation and publication are atomic with respect to
			// invalidation for this state segment. A pre-invalidation load may
			// still return to callers that already joined it, but cannot
			// repopulate the cache after the barrier.
			state.mu.RLock()

			if state.generation == generation {
				cache.storeLoaded(index, key, loaded, cache.store.elapsedAt(finishedAt))
			}

			state.mu.RUnlock()

			return loaded, nil
		},
	)

	state.mu.RUnlock()

	select {
	case <-ctx.Done():
		return zero, false, ctx.Err()

	case result := <-resultChannel:
		if result.Shared {
			cache.stats.recordShared(index)
		}

		if result.Err != nil {
			return zero, false, result.Err
		}

		loaded, ok := result.Val.(loadResult[V])
		if !ok {
			return zero, false, errors.New("pacecache: unexpected singleflight result type")
		}

		return loaded.value, loaded.found, nil
	}
}

// Set stores a positive value in the cache.
//
// DefaultExpiration uses the cache's configured positive TTL. A positive
// expiration overrides the configured TTL for this entry. A negative
// expiration disables time-based expiration.
//
// Set acts as a publication barrier: loads registered before Set may still
// return to callers already waiting for them, but cannot overwrite the
// explicitly stored value afterward.
func (cache *Cache[V]) Set(
	key string,
	value V,
	expiration time.Duration,
) {
	if !cache.initialized() {
		return
	}

	index := cache.store.segmentIndex(key)
	state := &cache.states[index]

	state.mu.Lock()

	state.generation++
	state.group.Forget(key)

	cache.storePositive(index, key, value, expiration, cache.store.now())

	state.mu.Unlock()
}

// Invalidate removes the specified keys from the cache.
//
// Invalidation acts as a publication barrier: a load registered before
// Invalidate may still complete for callers already waiting for it, but it
// cannot repopulate an invalidated key afterward.
//
// Missing keys are ignored. Duplicate keys are allowed.
//
// Invalidate is a no-op when called without keys or on an uninitialized Cache.
func (cache *Cache[V]) Invalidate(keys ...string) {
	if !cache.initialized() ||
		len(keys) == 0 {
		return
	}

	if len(keys) == 1 {
		index := cache.store.segmentIndex(keys[0])

		if cache.invalidateOne(index, keys[0]) {
			cache.stats.recordKeyInvalidation(index, 1)
		}

		return
	}

	targets := make([]invalidationTarget, len(keys))

	for index, key := range keys {
		targets[index] = invalidationTarget{
			key:   key,
			index: cache.store.segmentIndex(key),
		}
	}

	// Keep the statistics shard based on the caller's first key rather than
	// the sorted target order. The shard records the total number of resident
	// entries actually removed by this batch.
	statsIndex := targets[0].index

	// Every invalidation path acquires state locks in ascending segment order.
	// This keeps multi-key invalidation and InvalidateAll deadlock-free.
	slices.SortFunc(
		targets,
		func(left, right invalidationTarget) int {
			return cmp.Compare(left.index, right.index)
		},
	)

	previous := -1

	for _, target := range targets {
		if target.index == previous {
			continue
		}

		cache.states[target.index].mu.Lock()

		previous = target.index
	}

	// Once every affected state is locked, advance each generation. Loads that
	// registered before this barrier may finish for existing waiters, but they
	// cannot publish into any affected segment afterward.
	previous = -1

	for _, target := range targets {
		if target.index == previous {
			continue
		}

		cache.states[target.index].generation++

		previous = target.index
	}

	var invalidated int64

	for _, target := range targets {
		state := &cache.states[target.index]

		state.group.Forget(target.key)

		if cache.store.deleteAt(target.index, target.key) {
			invalidated++
		}
	}

	previous = -1

	for index := len(targets) - 1; index >= 0; index-- {
		target := targets[index]

		if target.index == previous {
			continue
		}

		cache.states[target.index].mu.Unlock()

		previous = target.index
	}

	cache.stats.recordKeyInvalidation(statsIndex, invalidated)
}

// InvalidateAll removes all entries from the cache.
//
// InvalidateAll acts as a cache-wide publication barrier. Loads registered
// before the barrier may still complete for callers already waiting for them,
// but they cannot repopulate the cache afterward.
//
// The removed entries may include physically resident entries whose TTL has
// already expired but which have not yet been removed.
//
// InvalidateAll is a no-op on an uninitialized Cache.
func (cache *Cache[V]) InvalidateAll() {
	if !cache.initialized() {
		return
	}

	for index := range cache.states {
		cache.states[index].mu.Lock()
	}

	for index := range cache.states {
		state := &cache.states[index]

		state.generation++
		state.group = &singleflight.Group{}
	}

	invalidated := cache.store.deleteAll()

	for index := len(cache.states) - 1; index >= 0; index-- {
		cache.states[index].mu.Unlock()
	}

	cache.stats.recordAllInvalidation(invalidated)
}

func (cache *Cache[V]) invalidateOne(index int, key string) bool {
	state := &cache.states[index]

	state.mu.Lock()

	state.generation++
	state.group.Forget(key)

	removed := cache.store.deleteAt(index, key)

	state.mu.Unlock()

	return removed
}

func (cache *Cache[V]) storePositive(
	index int,
	key string,
	value V,
	expiration time.Duration,
	now int64,
) {
	refreshTTL := cache.effectiveExpirationTTL(expiration)

	cache.store.setAt(
		index,
		key,
		cachedValue[V]{
			value:      value,
			found:      true,
			refreshTTL: refreshTTL,
		},
		deadlineAfter(now, refreshTTL),
		cache.stats.shard(index),
	)
}

func (cache *Cache[V]) storeLoaded(
	index int,
	key string,
	loaded loadResult[V],
	now int64,
) {
	switch {
	case loaded.found:
		cache.storePositive(index, key, loaded.value, DefaultExpiration, now)

	case cache.negativeTTL > 0:
		cache.store.setAt(
			index,
			key,
			cachedValue[V]{
				found: false,
			},
			deadlineAfter(now, cache.negativeTTL),
			cache.stats.shard(index),
		)
	}
}

func (cache *Cache[V]) effectiveExpirationTTL(expiration time.Duration) time.Duration {
	ttl := expiration

	if ttl == DefaultExpiration {
		ttl = cache.ttl
	}

	if ttl <= 0 {
		return 0
	}

	return jitteredTTL(ttl, cache.jitter)
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

func jitteredTTL(ttl time.Duration, jitter time.Duration) time.Duration {
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

func (cache *Cache[V]) registerMetrics(metrics Metrics) error {
	if metrics == nil {
		return nil
	}

	registration, err := metrics.RegisterCache(
		cacheStatsProvider[V]{
			cache: cache,
		},
	)
	if err != nil {
		return err
	}

	cache.metrics = registration

	return nil
}

func (cache *Cache[V]) initialized() bool {
	return cache != nil &&
		cache.store != nil &&
		cache.stats != nil
}

func newCacheStates(count int) []cacheState {
	states := make([]cacheState, count)

	for index := range states {
		states[index].group = &singleflight.Group{}
	}

	return states
}
