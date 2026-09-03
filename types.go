package pacecache

import (
	"context"
	"time"
)

// Entry is a read-only snapshot returned by GetEntry, GetOrLoadEntry,
// GetOrLoadEntryFunc, or GetOrSetEntry.
//
// Entry captures the value and expiration metadata observed by the operation.
// The value is returned using normal Go value semantics and is not deep-copied.
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

// Loader obtains one value for key from the underlying data source.
//
// found=true means the value exists and may be cached. found=false with a nil
// error means the value does not exist; not-found results are returned to the
// caller but are not stored by pacecache.
//
// Loader errors are never cached.
type Loader[K comparable, V any] func(ctx context.Context, key K) (value V, found bool, err error)

// loadResult carries the result of one shared cache lookup or loader invocation
// together with expiration metadata for a cached value.
type loadResult[V any] struct {
	value    V
	deadline int64
	found    bool
}

// cachedEntry is a storage snapshot used by entry-aware lookups.
type cachedEntry[V any] struct {
	value    V
	deadline int64
}
