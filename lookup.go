package pacecache

// Exists reports whether a live entry exists for key.
//
// Expired and missing entries return false. Exists does not update LRU recency,
// refresh sliding expiration, or affect lookup statistics.
//
// An expired entry observed by Exists is removed from storage and contributes
// to expiration statistics.
func (cache *Cache[K, V]) Exists(key K) bool {
	if !cache.initialized() {
		return false
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	return cache.store.existsAt(index, key, cache.store.now(), stats)
}

// RefreshTTL renews the expiration deadline of a live entry.
//
// The entry is refreshed using the effective TTL with which it was stored.
// RefreshTTL works independently of sliding expiration. An entry without
// time-based expiration is considered live and returns true without changing
// its state.
//
// Expired and missing entries return false. An expired entry observed by
// RefreshTTL is removed from storage and contributes to expiration statistics.
//
// RefreshTTL does not update LRU recency or lookup statistics.
func (cache *Cache[K, V]) RefreshTTL(key K) bool {
	if !cache.initialized() {
		return false
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	return cache.store.refreshTTLAt(index, key, cache.store.now(), stats)
}

// Get returns the cached value for key.
//
// The returned bool reports whether a live entry exists. A live hit updates LRU
// recency and contributes to lookup statistics. Expired entries are always
// treated as misses and are removed when observed, by DeleteExpired, or by
// background cleanup when it is enabled. When sliding expiration is enabled,
// a live hit refreshes the entry using the TTL with which it was stored.
func (cache *Cache[K, V]) Get(key K) (V, bool) {
	var zero V

	if !cache.initialized() {
		return zero, false
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	return cache.store.lookupAt(index, key, cache.store.now(), stats)
}

// GetEntry returns an immutable snapshot of the cached entry for key.
//
// GetEntry has the same lookup semantics as Get: it updates LRU recency,
// contributes to lookup statistics, removes an observed expired entry, and
// refreshes a live entry when sliding expiration is enabled.
//
// For an entry stored with NoExpiration, Entry.ExpiresAt returns the zero time.
// When sliding expiration refreshes an entry, ExpiresAt reflects the refreshed
// deadline.
func (cache *Cache[K, V]) GetEntry(key K) (Entry[V], bool) {
	var zero Entry[V]

	if !cache.initialized() {
		return zero, false
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	cached, found := cache.store.lookupEntryAt(index, key, cache.store.now(), stats)
	if !found {
		return zero, false
	}

	return Entry[V]{
		value:     cached.value,
		expiresAt: cache.store.expiresAt(cached.deadline),
	}, true
}
