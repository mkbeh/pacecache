package pacecache

// StatsProvider exposes one cache's identity and statistics to a metrics
// implementation. Cache implements StatsProvider.
type StatsProvider interface {
	Name() string
	Stats() Stats
}

// Metrics registers observability for a Cache.
//
// Implementations are expected to be immutable and safe to reuse for multiple
// caches. RegisterCache is called after the cache has been fully initialized.
type Metrics interface {
	RegisterCache(cache StatsProvider) (MetricsRegistration, error)
}

// MetricsRegistration owns a metrics registration associated with one Cache.
// Close is called once when the Cache is closed.
type MetricsRegistration interface {
	Close()
}
