package pacecache

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mkbeh/pacecache/internal/singleflight"
)

// cacheState coordinates loads and mutations for one storage segment.
//
// mu establishes ordering between singleflight registration, cache
// publication, and mutations. The singleflight group tracks publication state
// per active key so mutating one key never invalidates loads for another key in
// the same storage segment.
type cacheState[K comparable, V any] struct {
	mu    sync.RWMutex
	group singleflight.Group[K, loadResult[V]]
}

// GetOrLoad returns the cached value for key or obtains it from the configured
// default loader.
//
// On a cache miss, GetOrLoad returns ErrNoLoader when the cache was created
// without a default loader. Use NewWithDefaultLoader to configure one, or
// GetOrLoadWith to supply a loader for a specific operation.
//
// A loader result with found=true is cached using the configured TTL. A result
// with found=false and a nil error is returned to callers but is not cached.
// Loader errors are never cached.
//
// Concurrent misses for the same key are coalesced. The caller that starts the
// shared load executes the loader synchronously with its context. Other callers
// may stop waiting independently when their own contexts are canceled.
//
// Set, GetOrSet and GetOrSetEntry insertions, Delete, and Clear act as
// publication barriers for the affected key or keys. If a successful loader
// result is superseded by a newer cache mutation before publication, GetOrLoad
// discards the loader result and returns ErrLoadSuperseded. Loader errors take
// precedence over ErrLoadSuperseded. Mutations of other keys do not affect the
// load, even when those keys share the same segment.
//
// A loader must not call GetOrLoad, GetOrLoadWith, GetOrLoadEntry, or
// GetOrLoadEntryWith recursively for the same key, because the nested call
// would wait for the load already in progress.
//
// When err is non-nil, the returned value is the zero value of V and found is
// false.
func (cache *Cache[K, V]) GetOrLoad(
	ctx context.Context,
	key K,
) (V, bool, error) {
	if cache == nil {
		var zero V
		return zero, false, ErrNotInitialized
	}

	return cache.getOrLoadValue(ctx, key, cache.loader)
}

// GetOrLoadWith returns the cached value for key or obtains it from loader.
//
// The supplied loader is used instead of the cache's configured default loader
// when this caller starts the shared load. If another caller already owns a
// same-key load, GetOrLoadWith joins that wave and the supplied loader is not
// invoked. Loader is only required when no live cache entry exists; a nil
// loader therefore returns ErrNoLoader on a miss.
//
// GetOrLoadWith otherwise has the same cache, singleflight, publication, and
// statistics semantics as GetOrLoad.
func (cache *Cache[K, V]) GetOrLoadWith(
	ctx context.Context,
	key K,
	loader Loader[K, V],
) (V, bool, error) {
	return cache.getOrLoadValue(ctx, key, loader)
}

// GetOrLoadEntry returns a read-only cache entry snapshot for key or obtains
// the value from the configured default loader and publishes it before
// returning the snapshot.
//
// On a cache miss, GetOrLoadEntry returns ErrNoLoader when the cache was
// created without a default loader. Use NewWithDefaultLoader to configure one, or
// GetOrLoadEntryWith to supply a loader for a specific operation.
//
// A loader result with found=false is not cached and returns the zero Entry with
// found=false. GetOrLoadEntry otherwise has the same lookup, singleflight,
// publication-barrier, LRU, sliding-expiration, and statistics semantics as
// GetOrLoad.
//
// ExpiresAt reflects the exact deadline of the cached entry observed or
// published by the operation, including TTL jitter. As with GetEntry, the
// returned Entry is a snapshot and the cache may change immediately after the
// operation completes.
func (cache *Cache[K, V]) GetOrLoadEntry(
	ctx context.Context,
	key K,
) (Entry[V], bool, error) {
	if cache == nil {
		return Entry[V]{}, false, ErrNotInitialized
	}

	return cache.getOrLoadEntry(ctx, key, cache.loader)
}

// GetOrLoadEntryWith returns a read-only cache entry snapshot for key or
// obtains the value from loader and publishes it before returning the snapshot.
//
// The supplied loader is used instead of the cache's configured default loader
// when this caller starts the shared load. If another caller already owns a
// same-key load, GetOrLoadEntryWith joins that wave and the supplied loader is
// not invoked. Loader is only required when no live cache entry exists; a nil
// loader therefore returns ErrNoLoader on a miss.
//
// GetOrLoadEntryWith otherwise has the same semantics as GetOrLoadEntry.
func (cache *Cache[K, V]) GetOrLoadEntryWith(
	ctx context.Context,
	key K,
	loader Loader[K, V],
) (Entry[V], bool, error) {
	return cache.getOrLoadEntry(ctx, key, loader)
}

func (cache *Cache[K, V]) getOrLoadValue(
	ctx context.Context,
	key K,
	loader Loader[K, V],
) (V, bool, error) {
	result, err := cache.getOrLoad(ctx, key, loader)
	if err != nil {
		var zero V

		return zero, false, err
	}

	return result.value, result.found, nil
}

func (cache *Cache[K, V]) getOrLoadEntry(
	ctx context.Context,
	key K,
	loader Loader[K, V],
) (Entry[V], bool, error) {
	var zero Entry[V]

	result, err := cache.getOrLoad(ctx, key, loader)
	if err != nil {
		return zero, false, err
	}

	if !result.found {
		return zero, false, nil
	}

	return Entry[V]{
		value:     result.value,
		expiresAt: cache.store.expiresAt(result.deadline),
	}, true, nil
}

// getOrLoad implements the shared operation behind the default-loader and
// per-call-loader methods. All public load methods observe the same cache entry,
// execute the same singleflight wave, and linearize publication at the same
// point. The returned deadline is metadata from that same observed or published
// entry; callers that do not need it simply ignore it.
func (cache *Cache[K, V]) getOrLoad(
	ctx context.Context,
	key K,
	loader Loader[K, V],
) (loadResult[V], error) {
	if !cache.initialized() {
		return loadResult[V]{}, ErrNotInitialized
	}
	if ctx == nil {
		return loadResult[V]{}, errors.New("pacecache: context is nil")
	}

	index := cache.store.segmentIndex(key)
	stats := cache.stats.segment(index)

	if cached, ok := cache.store.lookupEntryAt(index, key, cache.store.now(), stats); ok {
		return loadResult[V]{
			value:    cached.value,
			deadline: cached.deadline,
			found:    true,
		}, nil
	}

	if err := ctx.Err(); err != nil {
		return loadResult[V]{}, err
	}

	if loader == nil {
		return loadResult[V]{}, ErrNoLoader
	}

	state := &cache.states[index]

	// Keep singleflight registration ordered with mutations. The owner executes
	// loader I/O only after releasing state.mu; duplicate callers wait outside
	// state.mu as well.
	state.mu.RLock()

	call, owner := state.group.StartCall(key)

	state.mu.RUnlock()

	if owner {
		state.group.DoCall(
			key,
			call,
			func(callState singleflight.CallState) (loadResult[V], error) {
				// The cache may have been populated between the initial lookup and
				// this call becoming the singleflight owner.
				if cached, ok := cache.store.getEntryAt(index, key, cache.store.now(), stats); ok {
					return loadResult[V]{
						value:    cached.value,
						deadline: cached.deadline,
						found:    true,
					}, nil
				}

				startedAt := cache.store.now()

				value, found, err := loader(ctx, key)

				finishedAt := cache.store.now()

				cache.stats.recordLoad(index, found, err, time.Duration(finishedAt-startedAt))

				if err != nil {
					return loadResult[V]{}, err
				}

				if !found {
					var zero V
					value = zero
				}

				loaded := loadResult[V]{
					value: value,
					found: found,
				}

				var refreshTTL time.Duration

				if found {
					refreshTTL = cache.effectiveTTL(DefaultExpiration)
					loaded.deadline = deadlineAfter(finishedAt, refreshTTL)
				}

				// Publication is ordered with cache mutations. A mutation that
				// wins first makes this successful load result stale for every waiter,
				// including a successful found=false result.
				state.mu.RLock()

				if callState.Forgotten() {
					state.mu.RUnlock()

					cache.stats.recordLoadSuperseded(index)

					return loadResult[V]{}, ErrLoadSuperseded
				}

				if loaded.found {
					cache.store.setAt(
						index,
						key,
						loaded.value,
						refreshTTL,
						loaded.deadline,
						stats,
					)
				}

				state.mu.RUnlock()

				return loaded, nil
			},
		)
	}

	result, err := call.Wait(ctx)
	if err != nil {
		return loadResult[V]{}, err
	}

	if result.Shared {
		cache.stats.recordShared(index)
	}

	if result.Err != nil {
		return loadResult[V]{}, result.Err
	}

	return result.Val, nil
}
