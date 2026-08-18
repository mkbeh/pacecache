package pacecache

import (
	"context"
	"time"
)

// LookupStatus describes the cache entry state observed by a lookup operation.
type LookupStatus uint8

const (
	// LookupMiss means no live cache entry exists for the key.
	LookupMiss LookupStatus = iota

	// LookupHit means a positive cache entry exists for the key.
	LookupHit

	// LookupNegativeHit means a cached not-found result exists for the key.
	LookupNegativeHit
)

// Entry is an immutable snapshot returned by GetEntry or GetOrLoadEntry.
//
// Value returns the cached value. For a negative cache entry, Value returns
// the zero value of V.
//
// ExpiresAt returns the entry expiration time. It returns the zero time for an
// entry stored with NoExpiration.
type Entry[V any] struct {
	value     V
	expiresAt time.Time
}

// Value returns the cached value captured by the operation that produced Entry.
func (entry Entry[V]) Value() V {
	return entry.value
}

// ExpiresAt returns the expiration time captured by the operation that produced Entry.
//
// The zero time means the entry has no time-based expiration.
func (entry Entry[V]) ExpiresAt() time.Time {
	return entry.expiresAt
}

// Loader obtains one value from the underlying data source.
//
// found=true means value exists and may be cached as a positive result.
// found=false with a nil error means the value does not exist and represents a
// successful negative result. Negative results are cached only when enabled
// with WithNegativeTTL.
//
// Loader errors are never cached.
type Loader[V any] func(ctx context.Context) (value V, found bool, err error)

// loadResult carries the value returned by a shared load together with the
// cache state observed or published by that same operation.
//
// LookupHit carries a positive value and its deadline. LookupNegativeHit
// carries the deadline of a cached negative result. LookupMiss represents a
// successful negative result that was not cached.
type loadResult[V any] struct {
	value    V
	deadline int64
	status   LookupStatus
}

func newLoadResult[V any](value V, found bool, deadline int64) loadResult[V] {
	if found {
		return loadResult[V]{
			value:    value,
			deadline: deadline,
			status:   LookupHit,
		}
	}

	if deadline > 0 {
		return loadResult[V]{
			deadline: deadline,
			status:   LookupNegativeHit,
		}
	}

	return loadResult[V]{
		status: LookupMiss,
	}
}

func loadResultFromCached[V any](cached cachedEntry[V]) loadResult[V] {
	status := LookupNegativeHit
	if cached.found {
		status = LookupHit
	}

	return loadResult[V]{
		value:    cached.value,
		deadline: cached.deadline,
		status:   status,
	}
}

type cachedValue[V any] struct {
	value V
	found bool

	// refreshTTL stores the effective positive TTL used by sliding expiration.
	// Any configured jitter has already been applied. Zero means that the entry
	// must not be refreshed by sliding expiration.
	refreshTTL time.Duration
}

// cachedEntry is an immutable storage snapshot used by entry-aware lookups.
type cachedEntry[V any] struct {
	value    V
	found    bool
	deadline int64
}
