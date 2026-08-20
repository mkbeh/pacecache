package pacecache

// StatsProvider exposes one cache's identity and statistics to a metrics
// implementation.
type StatsProvider interface {
	Name() string
	Stats() Stats
}

// Metrics registers metrics for a Cache.
//
// Implementations must be safe to reuse across multiple caches. RegisterCache
// may be called concurrently.
//
// If RegisterCache returns an error, the implementation must release any
// resources created during the registration attempt.
type Metrics interface {
	RegisterCache(cache StatsProvider) (MetricsRegistration, error)
}

// MetricsRegistration owns a metrics registration associated with one Cache.
// Close is called once when the Cache is closed.
type MetricsRegistration interface {
	Close()
}

// cacheStatsProvider exposes only the capabilities required by Metrics.
type cacheStatsProvider[K comparable, V any] struct {
	cache *Cache[K, V]
}

func (provider cacheStatsProvider[K, V]) Name() string {
	return provider.cache.Name()
}

func (provider cacheStatsProvider[K, V]) Stats() Stats {
	return provider.cache.Stats()
}
