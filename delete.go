package pacecache

import (
	"cmp"
	"slices"
)

type deletionTarget[K comparable] struct {
	key   K
	index int
}

// GetAndDelete atomically returns the live value for key and removes it
// from the cache. Missing and expired entries return the zero value with
// found=false. Expired entries are reclaimed as expirations.
//
// GetAndDelete acts as a publication barrier for the same key. An in-flight
// successful loader result is discarded with ErrLoadSuperseded if this method
// wins before publication, even when no resident entry exists.
//
// The operation does not refresh sliding expiration, update LRU recency, or
// affect lookup hit/miss statistics. A successfully removed live entry is
// counted as a deleted entry.
func (cache *Cache[K, V]) GetAndDelete(key K) (V, bool) {
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
		cache.stats.recordDelete(index, 1)
	}

	return value, found
}

// Delete removes the specified keys from the cache.
//
// Delete acts as a publication barrier for each key. A successful loader
// result already in flight for a deleted key is discarded with
// ErrLoadSuperseded if Delete wins before publication, and cannot repopulate
// that key afterward.
//
// Missing keys are ignored. Duplicate keys are allowed.
//
// Delete is a no-op when called without keys or on an uninitialized Cache.
func (cache *Cache[K, V]) Delete(keys ...K) {
	if !cache.initialized() || len(keys) == 0 {
		return
	}

	if len(keys) == 1 {
		index := cache.store.segmentIndex(keys[0])

		if cache.deleteOne(index, keys[0]) {
			cache.stats.recordDelete(index, 1)
		}

		return
	}

	if len(cache.states) == 1 {
		state := &cache.states[0]

		state.mu.Lock()

		var removed int64

		for _, key := range keys {
			state.group.Forget(key)

			if cache.store.deleteAt(0, key) {
				removed++
			}
		}

		state.mu.Unlock()

		cache.stats.recordDelete(0, removed)

		return
	}

	targets := make([]deletionTarget[K], len(keys))

	for index, key := range keys {
		targets[index] = deletionTarget[K]{
			key:   key,
			index: cache.store.segmentIndex(key),
		}
	}

	// Keep the statistics segment based on the caller's first key rather than
	// the sorted target order. The segment records the total number of resident
	// entries actually removed by this batch.
	statsIndex := targets[0].index

	// Every deletion path acquires state locks in ascending segment order.
	// This keeps multi-key deletion and Clear deadlock-free.
	slices.SortFunc(
		targets,
		func(left, right deletionTarget[K]) int {
			return cmp.Compare(left.index, right.index)
		},
	)

	previousLock := -1

	for _, target := range targets {
		if target.index == previousLock {
			continue
		}

		cache.states[target.index].mu.Lock()

		previousLock = target.index
	}

	var removed int64

	for _, target := range targets {
		state := &cache.states[target.index]

		state.group.Forget(target.key)

		if cache.store.deleteAt(target.index, target.key) {
			removed++
		}
	}

	previousUnlock := -1

	for _, target := range slices.Backward(targets) {
		if target.index == previousUnlock {
			continue
		}

		cache.states[target.index].mu.Unlock()

		previousUnlock = target.index
	}

	cache.stats.recordDelete(statsIndex, removed)
}

// Clear removes all entries from the cache.
//
// Clear acts as a cache-wide publication barrier. Successful loader results
// already in flight are discarded with ErrLoadSuperseded if Clear wins before
// publication, and cannot repopulate the cache afterward.
//
// The removed entries may include physically resident entries whose TTL has
// already expired but which have not yet been removed.
//
// Clear is a no-op on an uninitialized Cache.
func (cache *Cache[K, V]) Clear() {
	if !cache.initialized() {
		return
	}

	for index := range cache.states {
		cache.states[index].mu.Lock()
	}

	for index := range cache.states {
		cache.states[index].group.ForgetAll()
	}

	removed := cache.store.deleteAll()

	for index := len(cache.states) - 1; index >= 0; index-- {
		cache.states[index].mu.Unlock()
	}

	cache.stats.recordClear(removed)
}

// DeleteExpired physically removes expired entries using the cache expiration
// index and returns the number of entries removed.
//
// DeleteExpired is always available; background cleanup does not need to be
// enabled. Logical expiration is independent of physical cleanup: an expired
// entry is never returned even if it has not yet been reclaimed. Nearby
// expiration deadlines are grouped internally. Bucket eligibility may trail
// the exact TTL deadline by up to the internal bucket resolution; actual
// physical reclamation also depends on when cleanup runs. Manual cleanup uses
// the configured cleanup batch size and entry budget, yielding cooperatively
// between quanta until all entries due at the start of the call are drained.
func (cache *Cache[K, V]) DeleteExpired() int64 {
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

func (cache *Cache[K, V]) deleteOne(index int, key K) bool {
	state := &cache.states[index]

	state.mu.Lock()

	state.group.Forget(key)

	removed := cache.store.deleteAt(index, key)

	state.mu.Unlock()

	return removed
}
