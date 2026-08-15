package pacecache

import (
	"context"
	"time"
)

// LookupStatus describes the result of a local cache lookup.
type LookupStatus uint8

const (
	// LookupMiss means no live cache entry exists for the key.
	LookupMiss LookupStatus = iota

	// LookupHit means a positive cache entry exists for the key.
	LookupHit

	// LookupNegativeHit means a cached not-found result exists for the key.
	LookupNegativeHit
)

// Loader obtains one value from the underlying data source.
//
// found=true means value exists and may be cached as a positive result.
// found=false with a nil error means the value does not exist and represents a
// successful negative result. Negative results are cached only when enabled
// with WithNegativeTTL.
//
// Loader errors are never cached.
type Loader[V any] func(ctx context.Context) (value V, found bool, err error)

type loadResult[V any] struct {
	value V
	found bool
}

type cachedValue[V any] struct {
	value V
	found bool

	// refreshTTL stores the effective positive TTL used by sliding expiration.
	// Any configured jitter has already been applied. Zero means that the entry
	// must not be refreshed by sliding expiration.
	refreshTTL time.Duration
}
