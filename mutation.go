package pacecache

import (
	"cmp"
	"slices"
	"time"
)

type invalidationTarget[K comparable] struct {
	key   K
	index int
}

// Set stores a value in the cache.
//
// DefaultExpiration uses the cache's configured TTL. A positive expiration
// overrides the configured TTL for this entry. NoExpiration disables
// time-based expiration.
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

	refreshTTL := cache.effectiveTTL(expiration)

	var deadline int64

	if refreshTTL > 0 {
		deadline = deadlineAfter(cache.store.now(), refreshTTL)
	}

	cache.store.setAt(
		index,
		key,
		value,
		refreshTTL,
		deadline,
		cache.stats.segment(index),
	)

	state.mu.Unlock()
}

// GetOrSet returns the live value for key or atomically stores the provided
// value when no live entry exists.
//
// The returned bool reports whether an existing value was found. On a live
// hit, the provided value and expiration are ignored and the operation has the
// same LRU, sliding-expiration, and lookup-statistics semantics as Get. On a
// miss or expired entry, the provided value is stored using the same expiration
// semantics as Set and found=false is returned.
//
// When GetOrSet stores the provided value, it acts as a publication barrier for
// the same key. An in-flight successful loader result cannot overwrite the value
// inserted by GetOrSet afterward.
func (cache *Cache[K, V]) GetOrSet(
	key K,
	value V,
	expiration time.Duration,
) (V, bool) {
	var zero V

	if !cache.initialized() {
		return zero, false
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	// Keep the common existing-value path identical to Get and avoid the
	// coordination lock when no mutation is required.
	if current, found := cache.store.lookupAt(index, key, cache.store.now(), stats); found {
		return current, true
	}

	state := &cache.states[index]

	state.mu.Lock()

	// A concurrent load may have started after the initial miss. Forget it
	// before the atomic recheck/insert so a stale successful publication cannot
	// overwrite a value inserted by this operation.
	state.group.Forget(key)

	now := cache.store.now()
	refreshTTL := cache.effectiveTTL(expiration)

	current, found := cache.store.getOrSetAt(
		index,
		key,
		value,
		refreshTTL,
		deadlineAfter(now, refreshTTL),
		now,
		stats,
	)

	state.mu.Unlock()

	return current, found
}

// GetOrSetEntry returns the live entry for key or atomically stores the
// provided value when no live entry exists.
//
// The returned bool reports whether an existing entry was found. On a live
// hit, the provided value and expiration are ignored and the operation has the
// same LRU, sliding-expiration, and lookup-statistics semantics as GetEntry. On
// a miss or expired entry, the provided value is stored using the same
// expiration semantics as Set and found=false is returned. The returned Entry
// always describes the resident value selected by the operation, including the
// actual expiration deadline assigned to a newly inserted entry.
//
// When GetOrSetEntry stores the provided value, it acts as a publication
// barrier for the same key. An in-flight successful loader result cannot
// overwrite the value inserted by GetOrSetEntry afterward.
func (cache *Cache[K, V]) GetOrSetEntry(
	key K,
	value V,
	expiration time.Duration,
) (Entry[V], bool) {
	var zero Entry[V]

	if !cache.initialized() {
		return zero, false
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	// Keep the common existing-entry path identical to GetEntry and avoid the
	// coordination lock when no mutation is required.
	if cached, found := cache.store.lookupEntryAt(index, key, cache.store.now(), stats); found {
		return Entry[V]{
			value:     cached.value,
			expiresAt: cache.store.expiresAt(cached.deadline),
		}, true
	}

	state := &cache.states[index]

	state.mu.Lock()

	// A concurrent load may have started after the initial miss. Forget it
	// before the atomic recheck/insert so a stale successful publication cannot
	// overwrite a value inserted by this operation.
	state.group.Forget(key)

	now := cache.store.now()
	refreshTTL := cache.effectiveTTL(expiration)

	cached, found := cache.store.getOrSetEntryAt(
		index,
		key,
		value,
		refreshTTL,
		deadlineAfter(now, refreshTTL),
		now,
		stats,
	)

	state.mu.Unlock()

	return Entry[V]{
		value:     cached.value,
		expiresAt: cache.store.expiresAt(cached.deadline),
	}, found
}

// GetAndInvalidate atomically returns the live value for key and removes it
// from the cache. Missing and expired entries return the zero value with
// found=false. Expired entries are reclaimed as expirations.
//
// GetAndInvalidate acts as a publication barrier for the same key. An in-flight
// successful loader result is discarded with ErrLoadSuperseded if this method
// wins before publication, even when no resident entry exists.
//
// The operation does not refresh sliding expiration, update LRU recency, or
// affect lookup hit/miss statistics. A successfully removed live entry is
// counted as a key invalidation.
func (cache *Cache[K, V]) GetAndInvalidate(key K) (V, bool) {
	var zero V

	if !cache.initialized() {
		return zero, false
	}

	index := cache.store.segmentIndex(key)
	state := &cache.states[index]

	state.mu.Lock()

	state.group.Forget(key)

	value, found := cache.store.getAndDeleteAt(
		index,
		key,
		cache.store.now(),
		cache.stats.segment(index),
	)

	state.mu.Unlock()

	if found {
		cache.stats.recordKeyInvalidation(index, 1)
	}

	return value, found
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
	if !cache.initialized() || len(keys) == 0 {
		return
	}

	if len(keys) == 1 {
		index := cache.store.segmentIndex(keys[0])

		if cache.invalidateOne(index, keys[0]) {
			cache.stats.recordKeyInvalidation(index, 1)
		}

		return
	}

	if len(cache.states) == 1 {
		state := &cache.states[0]

		state.mu.Lock()

		var invalidated int64

		for _, key := range keys {
			state.group.Forget(key)

			if cache.store.deleteAt(0, key) {
				invalidated++
			}
		}

		state.mu.Unlock()

		cache.stats.recordKeyInvalidation(0, invalidated)

		return
	}

	targets := make([]invalidationTarget[K], len(keys))

	for index, key := range keys {
		targets[index] = invalidationTarget[K]{
			key:   key,
			index: cache.store.segmentIndex(key),
		}
	}

	// Keep the statistics segment based on the caller's first key rather than
	// the sorted target order. The segment records the total number of resident
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

	removed := cache.store.cleanupExpired(
		cache.store.now(),
		cache.cleanupPolicy,
		cache.stats,
	)

	cache.stats.recordCleanup()

	return removed
}

func (cache *Cache[K, V]) invalidateOne(index int, key K) bool {
	state := &cache.states[index]

	state.mu.Lock()

	state.group.Forget(key)

	removed := cache.store.deleteAt(index, key)

	state.mu.Unlock()

	return removed
}
