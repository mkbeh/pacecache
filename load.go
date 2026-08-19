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
	group *singleflight.Group[K, loadResult[V]]
}

// GetOrLoad returns the cached value for key or obtains it from loader.
//
// A loader result with found=true is cached using the configured TTL. A result
// with found=false and a nil error is returned to callers but is not cached.
// Loader errors are never cached.
//
// Concurrent misses for the same key are coalesced. The loader runs with the
// context of the caller that starts the shared load. Other callers may stop
// waiting independently when their own contexts are canceled.
//
// Set, a GetOrSet or GetOrSetEntry insertion, and invalidation act as publication barriers for
// the same key. If a successful loader result is superseded by a newer cache
// mutation before publication, GetOrLoad discards the loader result and returns
// ErrLoadSuperseded. Loader errors take precedence over
// ErrLoadSuperseded. Mutations of other keys do not affect the load, even when
// those keys share the same segment.
//
// When err is non-nil, the returned value is the zero value of V and found is
// false.
func (cache *Cache[K, V]) GetOrLoad(
	ctx context.Context,
	key K,
	loader Loader[V],
) (V, bool, error) {
	result, err := cache.getOrLoad(ctx, key, loader)
	if err != nil {
		var zero V

		return zero, false, err
	}

	return result.value, result.found, nil
}

// GetOrLoadEntry returns an immutable cache entry snapshot for key or obtains
// the value from loader and publishes it before returning the snapshot.
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
	loader Loader[V],
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

// getOrLoad implements the shared operation behind GetOrLoad and
// GetOrLoadEntry. Both public methods observe the same cache entry, execute the
// same singleflight wave, and linearize publication at the same point. The
// returned deadline is metadata from that same observed or published entry;
// callers that do not need it simply ignore it.
func (cache *Cache[K, V]) getOrLoad(
	ctx context.Context,
	key K,
	loader Loader[V],
) (loadResult[V], error) {
	if !cache.initialized() {
		return loadResult[V]{}, errors.New("pacecache: cache is not initialized")
	}
	if ctx == nil {
		return loadResult[V]{}, errors.New("pacecache: context is nil")
	}
	if loader == nil {
		return loadResult[V]{}, errors.New("pacecache: loader is nil")
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

	state := &cache.states[index]

	// Keep singleflight registration ordered with mutations. Do only registers
	// or starts the shared call; loader I/O and caller waiting run outside
	// state.mu.
	state.mu.RLock()

	call := state.group.Do(
		key,
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

			value, found, err := loader(ctx)

			finishedAt := cache.store.now()

			cache.stats.recordLoad(index, found, err, time.Duration(finishedAt-startedAt))

			if err != nil {
				return loadResult[V]{}, err
			}

			if !found {
				var zero V
				value = zero
			}

			var (
				deadline   int64
				refreshTTL time.Duration
			)

			if found {
				refreshTTL = cache.effectiveExpirationTTL(DefaultExpiration)
				deadline = deadlineAfter(finishedAt, refreshTTL)
			}

			loaded := loadResult[V]{
				value:    value,
				deadline: deadline,
				found:    found,
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

			cache.storeLoaded(index, key, loaded, refreshTTL)

			state.mu.RUnlock()

			return loaded, nil
		},
	)

	state.mu.RUnlock()

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

func (cache *Cache[K, V]) storeLoaded(
	index int,
	key K,
	loaded loadResult[V],
	refreshTTL time.Duration,
) {
	if !loaded.found {
		return
	}

	cache.store.setAt(
		index,
		key,
		loaded.value,
		refreshTTL,
		loaded.deadline,
		cache.stats.segment(index),
	)
}

func newCacheStates[K comparable, V any](count int) []cacheState[K, V] {
	states := make([]cacheState[K, V], count)

	for index := range states {
		states[index].group = &singleflight.Group[K, loadResult[V]]{}
	}

	return states
}
