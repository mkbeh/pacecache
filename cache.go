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

	"github.com/mkbeh/pacecache/internal/singleflight"
)

const (
	// DefaultExpiration uses the cache's configured positive TTL.
	DefaultExpiration time.Duration = 0

	// NoExpiration disables time-based expiration for the entry.
	NoExpiration time.Duration = -1
)

// Cache is a bounded in-process read-through cache for keys of type K and values of type V.
//
// Cache uses segmented exact LRU eviction and TTL expiration. Positive entries
// may optionally use sliding expiration, while negative results always use
// absolute expiration. Concurrent loads for the same key are coalesced through
// singleflight.
//
// Cache is safe for concurrent use. A Cache must not be copied after creation.
type Cache[K comparable, V any] struct {
	name string

	store  *storage[K, V]
	states []cacheState[K, V]
	stats  *statsCollector

	cleanupPolicy cleanupPolicy
	cleanup       *cleanupWorker[K, V]

	metrics   MetricsRegistration
	closeOnce sync.Once

	ttl         time.Duration
	jitter      time.Duration
	negativeTTL time.Duration
}

// cacheState coordinates loads and mutations for one storage segment.
//
// mu establishes ordering between singleflight registration, cache
// publication, and mutations. The singleflight group tracks publication state
// per active key so mutating one key never invalidates loads for another key in
// the same storage segment.
type cacheState[K comparable, V any] struct {
	mu sync.RWMutex

	group *singleflight.Group[K, loadResult[V]]
}

type invalidationTarget[K comparable] struct {
	key   K
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
func New[K comparable, V any](name string, options ...Option) (*Cache[K, V], error) {
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
		name: settings.name,

		store:  store,
		states: newCacheStates[K, V](len(store.segments)),
		stats:  newStatsCollector(len(store.segments)),

		cleanupPolicy: policy,

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

// Exists reports whether a live positive entry exists for key.
//
// Negative, expired, and missing entries return false. Exists does not update
// LRU recency, refresh sliding expiration, or affect lookup statistics.
//
// An expired entry observed by Exists is removed from storage and contributes
// to expiration statistics.
func (cache *Cache[K, V]) Exists(key K) bool {
	if !cache.initialized() {
		return false
	}

	index := cache.store.segmentIndex(key)

	return cache.store.existsAt(
		index,
		key,
		cache.store.now(),
		cache.stats.segment(index),
	)
}

// RefreshTTL renews the expiration deadline of a live positive entry.
//
// The entry is refreshed using the effective TTL with which it was stored.
// RefreshTTL works independently of sliding expiration. A positive entry
// without time-based expiration is considered live and returns true without
// changing its state.
//
// Negative, expired, and missing entries return false. An expired entry
// observed by RefreshTTL is removed from storage and contributes to expiration
// statistics.
//
// RefreshTTL does not update LRU recency or lookup statistics.
func (cache *Cache[K, V]) RefreshTTL(key K) bool {
	if !cache.initialized() {
		return false
	}

	index := cache.store.segmentIndex(key)

	return cache.store.refreshTTLAt(
		index,
		key,
		cache.store.now(),
		cache.stats.segment(index),
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
func (cache *Cache[K, V]) Get(key K) (V, LookupStatus) {
	var zero V

	if !cache.initialized() {
		return zero, LookupMiss
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	cached, ok := cache.store.lookupAt(index, key, cache.store.now(), stats)
	if !ok {
		return zero, LookupMiss
	}

	if !cached.found {
		return zero, LookupNegativeHit
	}

	return cached.value, LookupHit
}

// GetEntry returns an immutable snapshot of the cached entry for key.
//
// GetEntry has the same lookup semantics as Get: it updates LRU recency,
// contributes to lookup statistics, removes an observed expired entry, and
// refreshes a live positive entry when sliding expiration is enabled.
//
// LookupHit returns a positive entry. LookupNegativeHit returns an entry whose
// Value is the zero value of V and whose ExpiresAt reports the negative entry's
// expiration. LookupMiss returns the zero Entry.
//
// For an entry stored with NoExpiration, Entry.ExpiresAt returns the zero time.
// When sliding expiration refreshes an entry, ExpiresAt reflects the refreshed
// deadline.
func (cache *Cache[K, V]) GetEntry(key K) (Entry[V], LookupStatus) {
	var zero Entry[V]

	if !cache.initialized() {
		return zero, LookupMiss
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	cached, ok := cache.store.lookupEntryAt(index, key, cache.store.now(), stats)
	if !ok {
		return zero, LookupMiss
	}

	expiresAt := cache.store.expiresAt(cached.deadline)

	if !cached.found {
		return Entry[V]{
			expiresAt: expiresAt,
		}, LookupNegativeHit
	}

	return Entry[V]{
		value:     cached.value,
		expiresAt: expiresAt,
	}, LookupHit
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
// Set and invalidation act as publication barriers for the same key. If a
// successful loader result is superseded by a newer Set, Invalidate, or
// InvalidateAll operation before publication, GetOrLoad discards the loader
// result and returns ErrLoadSuperseded. Loader errors take precedence over
// ErrLoadSuperseded. Mutations of other keys do not affect the load, even when
// those keys share the same segment.
//
// The returned found value describes whether the underlying value exists; it
// is false for both a freshly loaded and a cached negative result. When err is
// non-nil, the returned value is the zero value of V and found is false.
func (cache *Cache[K, V]) GetOrLoad(
	ctx context.Context,
	key K,
	loader Loader[V],
) (V, bool, error) {
	result, err := cache.getOrLoad(ctx, key, loader)
	if err != nil {
		var zero V

		return zero, false, err
	}

	return result.value, result.status == LookupHit, nil
}

// GetOrLoadEntry returns an immutable cache entry snapshot for key or obtains
// the value from loader and publishes it before returning the snapshot.
//
// LookupHit is returned for a positive cached or successfully published value.
// LookupNegativeHit is returned when a cached negative entry exists or a
// negative loader result is published because negative caching is enabled.
// If loader reports found=false while negative caching is disabled, no cache
// entry is created and GetOrLoadEntry returns the zero Entry with LookupMiss.
//
// GetOrLoadEntry has the same lookup, singleflight, publication-barrier, LRU,
// sliding-expiration, and statistics semantics as GetOrLoad. The two methods
// differ only in the shape of the returned result.
//
// ExpiresAt reflects the exact deadline of the cached entry observed or
// published by the operation, including TTL jitter. As with GetEntry, the
// returned Entry is a snapshot and the cache may change immediately after the
// operation completes.
func (cache *Cache[K, V]) GetOrLoadEntry(
	ctx context.Context,
	key K,
	loader Loader[V],
) (Entry[V], LookupStatus, error) {
	var zero Entry[V]

	result, err := cache.getOrLoad(ctx, key, loader)
	if err != nil {
		return zero, LookupMiss, err
	}

	status := result.status
	if status == LookupMiss {
		return zero, LookupMiss, nil
	}

	expiresAt := cache.store.expiresAt(result.deadline)

	if status == LookupNegativeHit {
		return Entry[V]{
			expiresAt: expiresAt,
		}, LookupNegativeHit, nil
	}

	return Entry[V]{
		value:     result.value,
		expiresAt: expiresAt,
	}, LookupHit, nil
}

// getOrLoad implements the shared operation behind GetOrLoad and
// GetOrLoadEntry. Both public methods observe the same cache entry, execute the
// same singleflight wave, and linearize publication at the same point. The
// returned deadline is metadata from that same observed or published entry;
// callers that do not need it simply ignore it.
func (cache *Cache[K, V]) getOrLoad(
	ctx context.Context,
	key K,
	loader Loader[V],
) (loadResult[V], error) {
	if !cache.initialized() {
		return loadResult[V]{}, errors.New("pacecache: cache is not initialized")
	}

	if ctx == nil {
		return loadResult[V]{}, errors.New("pacecache: context is nil")
	}

	if loader == nil {
		return loadResult[V]{}, errors.New("pacecache: loader is nil")
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	if cached, ok := cache.store.lookupEntryAt(index, key, cache.store.now(), stats); ok {
		return loadResultFromCached(cached), nil
	}

	state := &cache.states[index]

	// Keep singleflight registration ordered with mutations. DoChan only
	// registers or starts the shared call; loader I/O runs outside state.mu.
	state.mu.RLock()

	resultChannel := state.group.DoChan(
		key,
		func(callState singleflight.CallState) (loadResult[V], error) {
			// Another caller may have populated the cache between the initial
			// lookup and this call becoming the singleflight owner. This lookup
			// observes the same value and deadline regardless of which public API
			// started the wave.
			if cached, ok := cache.store.getEntryAt(index, key, cache.store.now(), stats); ok {
				return loadResultFromCached(cached), nil
			}

			startedAt := time.Now()

			value, found, err := loader(ctx)

			finishedAt := time.Now()

			cache.stats.recordLoad(index, found, err, finishedAt.Sub(startedAt))

			if err != nil {
				return loadResult[V]{}, err
			}

			if !found {
				var zeroValue V

				value = zeroValue
			}

			now := cache.store.elapsedAt(finishedAt)

			var (
				deadline   int64
				refreshTTL time.Duration
			)

			if found {
				refreshTTL = cache.effectiveExpirationTTL(DefaultExpiration)
				deadline = deadlineAfter(now, refreshTTL)
			} else if cache.negativeTTL > 0 {
				deadline = deadlineAfter(now, cache.negativeTTL)
			}

			loaded := newLoadResult(value, found, deadline)

			// Publication is atomic with respect to Set and invalidation. If a
			// mutation forgot this call before publication, every waiter observes
			// ErrLoadSuperseded rather than a stale loader result.
			state.mu.RLock()

			if callState.Forgotten() {
				state.mu.RUnlock()

				cache.stats.recordLoadSuperseded(index)

				return loadResult[V]{}, ErrLoadSuperseded
			}

			cache.storeLoaded(index, key, loaded, refreshTTL)

			state.mu.RUnlock()

			return loaded, nil
		},
	)

	state.mu.RUnlock()

	select {
	case <-ctx.Done():
		return loadResult[V]{}, ctx.Err()

	case result := <-resultChannel:
		if result.Shared {
			cache.stats.recordShared(index)
		}

		if result.Err != nil {
			return loadResult[V]{}, result.Err
		}

		return result.Val, nil
	}
}

// Set stores a positive value in the cache.
//
// DefaultExpiration uses the cache's configured positive TTL. A positive
// expiration overrides the configured TTL for this entry. A negative
// expiration disables time-based expiration.
//
// Set acts as a publication barrier. A successful loader result that was
// already in flight for the same key is discarded with ErrLoadSuperseded if
// Set wins before publication, and cannot overwrite the explicitly stored
// value afterward.
func (cache *Cache[K, V]) Set(
	key K,
	value V,
	expiration time.Duration,
) {
	if !cache.initialized() {
		return
	}

	index := cache.store.segmentIndex(key)
	state := &cache.states[index]

	state.mu.Lock()

	state.group.Forget(key)

	cache.storePositive(index, key, value, expiration, cache.store.now())

	state.mu.Unlock()
}

// Invalidate removes the specified keys from the cache.
//
// Invalidation acts as a publication barrier. A successful loader result that
// was already in flight for an invalidated key is discarded with
// ErrLoadSuperseded if Invalidate wins before publication, and cannot repopulate
// the key afterward.
//
// Missing keys are ignored. Duplicate keys are allowed.
//
// Invalidate is a no-op when called without keys or on an uninitialized Cache.
func (cache *Cache[K, V]) Invalidate(keys ...K) {
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

	targets := make([]invalidationTarget[K], len(keys))

	for index, key := range keys {
		targets[index] = invalidationTarget[K]{
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
		func(left, right invalidationTarget[K]) int {
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
// InvalidateAll acts as a cache-wide publication barrier. Successful loader
// results already in flight are discarded with ErrLoadSuperseded if
// InvalidateAll wins before publication, and cannot repopulate the cache
// afterward.
//
// The removed entries may include physically resident entries whose TTL has
// already expired but which have not yet been removed.
//
// InvalidateAll is a no-op on an uninitialized Cache.
func (cache *Cache[K, V]) InvalidateAll() {
	if !cache.initialized() {
		return
	}

	for index := range cache.states {
		cache.states[index].mu.Lock()
	}

	for index := range cache.states {
		cache.states[index].group.ForgetAll()
	}

	invalidated := cache.store.deleteAll()

	for index := len(cache.states) - 1; index >= 0; index-- {
		cache.states[index].mu.Unlock()
	}

	cache.stats.recordAllInvalidation(invalidated)
}

// CleanupExpired physically removes expired entries using the cache expiration
// index and returns the number of entries removed.
//
// CleanupExpired is always available; background cleanup does not need to be
// enabled. Logical expiration is independent of physical cleanup: an expired
// entry is never returned even if it has not yet been reclaimed. Nearby
// expiration deadlines are grouped internally. Bucket eligibility may trail
// the exact TTL deadline by up to the internal bucket resolution; actual
// physical reclamation also depends on when cleanup runs. Manual cleanup uses
// the configured cleanup batch size and entry budget, yielding cooperatively
// between quanta until all entries due at the start of the call are drained.
func (cache *Cache[K, V]) CleanupExpired() int64 {
	if !cache.initialized() {
		return 0
	}

	return cache.store.cleanupExpired(
		cache.store.now(),
		cache.cleanupPolicy,
		cache.stats,
	)
}

func (cache *Cache[K, V]) invalidateOne(index int, key K) bool {
	state := &cache.states[index]

	state.mu.Lock()

	state.group.Forget(key)

	removed := cache.store.deleteAt(index, key)

	state.mu.Unlock()

	return removed
}

func (cache *Cache[K, V]) storeLoaded(
	index int,
	key K,
	loaded loadResult[V],
	refreshTTL time.Duration,
) {
	if loaded.status == LookupMiss {
		return
	}

	found := loaded.status == LookupHit

	cached := cachedValue[V]{
		found: found,
	}

	if found {
		cached.value = loaded.value
		cached.refreshTTL = refreshTTL
	}

	cache.store.setAt(
		index,
		key,
		cached,
		loaded.deadline,
		cache.stats.segment(index),
	)
}

func (cache *Cache[K, V]) storePositive(
	index int,
	key K,
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
		cache.stats.segment(index),
	)
}

func (cache *Cache[K, V]) effectiveExpirationTTL(expiration time.Duration) time.Duration {
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

func newCacheStates[K comparable, V any](count int) []cacheState[K, V] {
	states := make([]cacheState[K, V], count)

	for index := range states {
		states[index].group = &singleflight.Group[K, loadResult[V]]{}
	}

	return states
}
