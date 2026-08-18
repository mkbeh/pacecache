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

const uncachedNegativeLoadState int64 = -1 << 63

// loadResult carries the value returned by a shared load together with the
// cache state observed or published by that same operation. The state is
// encoded in one int64 to keep the singleflight payload compact.
//
//	state >= 0: positive entry; state is its deadline (zero means no expiry)
//	MinInt64:    successful negative result that was not cached
//	otherwise:   cached negative entry; -state is its deadline
//
// Negative cache entries always have a positive deadline, so every state has
// an unambiguous meaning.
type loadResult[V any] struct {
	value V
	state int64
}

func newLoadResult[V any](value V, found bool, deadline int64) loadResult[V] {
	if found {
		return loadResult[V]{
			value: value,
			state: deadline,
		}
	}

	if deadline <= 0 {
		return loadResult[V]{
			state: uncachedNegativeLoadState,
		}
	}

	return loadResult[V]{
		state: -deadline,
	}
}

func loadResultFromCached[V any](cached cachedEntry[V]) loadResult[V] {
	return newLoadResult(
		cached.value,
		cached.found,
		cached.deadline,
	)
}

func (result loadResult[V]) found() bool {
	return result.state >= 0
}

func (result loadResult[V]) status() LookupStatus {
	switch {
	case result.state >= 0:
		return LookupHit
	case result.state == uncachedNegativeLoadState:
		return LookupMiss
	default:
		return LookupNegativeHit
	}
}

func (result loadResult[V]) deadline() int64 {
	switch {
	case result.state >= 0:
		return result.state
	case result.state == uncachedNegativeLoadState:
		return 0
	default:
		return -result.state
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

// cachedEntry is an immutable storage snapshot used by GetEntry.
type cachedEntry[V any] struct {
	value    V
	found    bool
	deadline int64
}
