package pacecache

import "time"

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
