package pacecache

// MetricsSource exposes cache identity and statistics to a metrics implementation.
type MetricsSource interface {
	Name() string
	Stats() Stats
}

// Metrics registers cache statistics with a metrics implementation.
//
// Implementations must be safe to reuse across multiple caches. Register may
// be called concurrently.
//
// If Register returns an error, the implementation must release any resources
// created during the registration attempt.
type Metrics interface {
	Register(source MetricsSource) error
}

// metricsSource exposes only the capabilities required by Metrics.
type metricsSource[K comparable, V any] struct {
	name  string
	cache *Cache[K, V]
}

func (source metricsSource[K, V]) Name() string {
	return source.name
}

func (source metricsSource[K, V]) Stats() Stats {
	return source.cache.Stats()
}
