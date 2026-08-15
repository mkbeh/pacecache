package pacecache

// StatsProvider exposes one cache's identity and statistics to a metrics
// implementation.
type StatsProvider interface {
	Name() string
	Stats() Stats
}

// Metrics registers observability for a Cache.
//
// Implementations are expected to be immutable and safe to reuse for multiple
// caches.
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
type cacheStatsProvider[V any] struct {
	cache *Cache[V]
}

func (provider cacheStatsProvider[V]) Name() string {
	return provider.cache.Name()
}

func (provider cacheStatsProvider[V]) Stats() Stats {
	return provider.cache.Stats()
}
