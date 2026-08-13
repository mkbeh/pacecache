package pacecache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCachePositiveResultIsCached(t *testing.T) {
	cache := newTestCache[int](t)
	var loads atomic.Int64

	loader := func(context.Context) (int, bool, error) {
		loads.Add(1)
		return 42, true, nil
	}

	for call := 0; call < 2; call++ {
		value, found, err := cache.GetOrLoad(context.Background(), "answer", loader)
		if err != nil {
			t.Fatalf("GetOrLoad() error = %v", err)
		}
		if !found || value != 42 {
			t.Fatalf("GetOrLoad() = (%d, %t), want (42, true)", value, found)
		}
	}

	if loads.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", loads.Load())
	}

	stats := cache.Stats()
	if stats.MissCount != 1 || stats.HitCount != 1 || stats.LoadFoundCount != 1 {
		t.Fatalf("Stats() = %+v, want one miss, one hit, one found load", stats)
	}
}

func TestCacheNegativeResultIsCachedWhenEnabled(t *testing.T) {
	cache := newTestCache[int](t, WithNegativeTTL(time.Minute))
	var loads atomic.Int64

	loader := func(context.Context) (int, bool, error) {
		loads.Add(1)
		return 99, false, nil
	}

	for call := 0; call < 2; call++ {
		value, found, err := cache.GetOrLoad(context.Background(), "missing", loader)
		if err != nil {
			t.Fatalf("GetOrLoad() error = %v", err)
		}
		if found || value != 0 {
			t.Fatalf("GetOrLoad() = (%d, %t), want (0, false)", value, found)
		}
	}

	if loads.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", loads.Load())
	}

	stats := cache.Stats()
	if stats.MissCount != 1 || stats.NegativeHitCount != 1 || stats.LoadNotFoundCount != 1 {
		t.Fatalf("Stats() = %+v, want one miss, one negative hit, one not-found load", stats)
	}
}

func TestCacheNegativeResultIsNotCachedByDefault(t *testing.T) {
	cache := newTestCache[int](t)
	var loads atomic.Int64

	loader := func(context.Context) (int, bool, error) {
		loads.Add(1)
		return 0, false, nil
	}

	for call := 0; call < 2; call++ {
		_, found, err := cache.GetOrLoad(context.Background(), "missing", loader)
		if err != nil {
			t.Fatalf("GetOrLoad() error = %v", err)
		}
		if found {
			t.Fatal("GetOrLoad() found = true, want false")
		}
	}

	if loads.Load() != 2 {
		t.Fatalf("loader calls = %d, want 2", loads.Load())
	}

	stats := cache.Stats()
	if stats.MissCount != 2 || stats.LoadNotFoundCount != 2 || stats.EntryCount != 0 {
		t.Fatalf("Stats() = %+v, want two misses, two not-found loads, zero entries", stats)
	}
}

func TestCacheLoaderErrorsAreNotCached(t *testing.T) {
	cache := newTestCache[int](t)
	sentinel := errors.New("load failed")
	var loads atomic.Int64

	loader := func(context.Context) (int, bool, error) {
		loads.Add(1)
		return 0, false, sentinel
	}

	for call := 0; call < 2; call++ {
		_, _, err := cache.GetOrLoad(context.Background(), "key", loader)
		if !errors.Is(err, sentinel) {
			t.Fatalf("GetOrLoad() error = %v, want sentinel", err)
		}
	}

	if loads.Load() != 2 {
		t.Fatalf("loader calls = %d, want 2", loads.Load())
	}

	stats := cache.Stats()
	if stats.LoadErrorCount != 2 || stats.EntryCount != 0 {
		t.Fatalf("Stats() = %+v, want two load errors and zero entries", stats)
	}
}

func TestCacheRejectsNilLoader(t *testing.T) {
	cache := newTestCache[int](t)

	_, _, err := cache.GetOrLoad(context.Background(), "key", nil)
	if err == nil || err.Error() != "pacecache: loader is nil" {
		t.Fatalf("GetOrLoad() error = %v, want nil-loader error", err)
	}
}

func TestUninitializedCacheRejectsGetOrLoad(t *testing.T) {
	var cache *Cache[int]

	_, _, err := cache.GetOrLoad(
		context.Background(),
		"key",
		func(context.Context) (int, bool, error) { return 1, true, nil },
	)
	if err == nil || err.Error() != "pacecache: cache is not initialized" {
		t.Fatalf("GetOrLoad() error = %v, want uninitialized-cache error", err)
	}
}

func TestCacheInvalidateForcesReload(t *testing.T) {
	cache := newTestCache[int](t)
	var loads atomic.Int64

	loader := func(context.Context) (int, bool, error) {
		return int(loads.Add(1)), true, nil
	}

	first, _, err := cache.GetOrLoad(context.Background(), "key", loader)
	if err != nil {
		t.Fatalf("first GetOrLoad() error = %v", err)
	}

	cache.Invalidate("key")

	second, _, err := cache.GetOrLoad(context.Background(), "key", loader)
	if err != nil {
		t.Fatalf("second GetOrLoad() error = %v", err)
	}

	if first != 1 || second != 2 {
		t.Fatalf("values = (%d, %d), want (1, 2)", first, second)
	}
	if stats := cache.Stats(); stats.InvalidatedKeyCount != 1 {
		t.Fatalf("InvalidatedKeyCount = %d, want 1", stats.InvalidatedKeyCount)
	}
}

func TestCacheInvalidateIgnoresMissingAndDuplicateKeys(t *testing.T) {
	cache := newTestCache[int](t)

	for _, key := range []string{"a", "b"} {
		_, _, err := cache.GetOrLoad(
			context.Background(),
			key,
			func(context.Context) (int, bool, error) { return 1, true, nil },
		)
		if err != nil {
			t.Fatalf("GetOrLoad(%q) error = %v", key, err)
		}
	}

	cache.Invalidate("a", "a", "missing", "b")

	stats := cache.Stats()
	if stats.InvalidatedKeyCount != 2 {
		t.Fatalf("InvalidatedKeyCount = %d, want 2", stats.InvalidatedKeyCount)
	}
	if stats.EntryCount != 0 {
		t.Fatalf("EntryCount = %d, want 0", stats.EntryCount)
	}
}

func TestCacheInvalidateAllForcesReload(t *testing.T) {
	cache := newTestCache[int](t)
	var loads atomic.Int64

	for _, key := range []string{"a", "b", "c"} {
		_, _, err := cache.GetOrLoad(
			context.Background(),
			key,
			func(context.Context) (int, bool, error) {
				return int(loads.Add(1)), true, nil
			},
		)
		if err != nil {
			t.Fatalf("GetOrLoad(%q) error = %v", key, err)
		}
	}

	cache.InvalidateAll()

	stats := cache.Stats()
	if stats.EntryCount != 0 {
		t.Fatalf("EntryCount = %d, want 0", stats.EntryCount)
	}
	if stats.InvalidatedAllCount != 3 {
		t.Fatalf("InvalidatedAllCount = %d, want 3", stats.InvalidatedAllCount)
	}
}

func TestCacheConcurrentMissesAreCoalescedAndWaitersCancelIndependently(t *testing.T) {
	cache := newTestCache[int](t)

	started := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int64

	loader := func(ctx context.Context) (int, bool, error) {
		loads.Add(1)
		close(started)

		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()

		case <-release:
			return 42, true, nil
		}
	}

	ownerResult := make(chan error, 1)
	go func() {
		value, found, err := cache.GetOrLoad(context.Background(), "key", loader)
		if err == nil && (!found || value != 42) {
			err = fmt.Errorf("owner result = (%d, %t)", value, found)
		}
		ownerResult <- err
	}()

	<-started

	waiterCtx, cancel := context.WithCancel(context.Background())
	waiterResult := make(chan error, 1)
	go func() {
		_, _, err := cache.GetOrLoad(waiterCtx, "key", loader)
		waiterResult <- err
	}()

	cancel()

	if err := <-waiterResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error = %v, want context.Canceled", err)
	}

	close(release)
	if err := <-ownerResult; err != nil {
		t.Fatalf("owner error = %v", err)
	}

	if loads.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", loads.Load())
	}

	stats := cache.Stats()
	if stats.LoadFoundCount != 1 {
		t.Fatalf("LoadFoundCount = %d, want 1", stats.LoadFoundCount)
	}
	if stats.SharedCount != 1 {
		t.Fatalf("SharedCount = %d, want 1", stats.SharedCount)
	}
}

func TestCacheInvalidatePreventsInFlightLoadFromRepopulating(t *testing.T) {
	cache := newTestCache[int](t)

	started := make(chan struct{})
	release := make(chan struct{})
	firstResult := make(chan error, 1)

	go func() {
		value, found, err := cache.GetOrLoad(
			context.Background(),
			"key",
			func(context.Context) (int, bool, error) {
				close(started)
				<-release
				return 1, true, nil
			},
		)
		if err == nil && (!found || value != 1) {
			err = fmt.Errorf("first result = (%d, %t)", value, found)
		}
		firstResult <- err
	}()

	<-started
	cache.Invalidate("key")
	close(release)

	if err := <-firstResult; err != nil {
		t.Fatalf("first GetOrLoad() error = %v", err)
	}

	var secondLoads atomic.Int64
	value, found, err := cache.GetOrLoad(
		context.Background(),
		"key",
		func(context.Context) (int, bool, error) {
			secondLoads.Add(1)
			return 2, true, nil
		},
	)
	if err != nil {
		t.Fatalf("second GetOrLoad() error = %v", err)
	}
	if !found || value != 2 {
		t.Fatalf("second result = (%d, %t), want (2, true)", value, found)
	}
	if secondLoads.Load() != 1 {
		t.Fatalf("second loader calls = %d, want 1", secondLoads.Load())
	}
}

func TestCacheInvalidateAllowsFreshLoadBeforeOldLoadCompletes(t *testing.T) {
	cache := newTestCache[int](t)

	oldStarted := make(chan struct{})
	oldRelease := make(chan struct{})
	oldResult := make(chan error, 1)

	go func() {
		value, found, err := cache.GetOrLoad(
			context.Background(),
			"key",
			func(context.Context) (int, bool, error) {
				close(oldStarted)
				<-oldRelease
				return 1, true, nil
			},
		)
		if err == nil && (!found || value != 1) {
			err = fmt.Errorf("old result = (%d, %t)", value, found)
		}
		oldResult <- err
	}()

	<-oldStarted
	cache.Invalidate("key")

	fresh, found, err := cache.GetOrLoad(
		context.Background(),
		"key",
		func(context.Context) (int, bool, error) { return 2, true, nil },
	)
	if err != nil {
		t.Fatalf("fresh GetOrLoad() error = %v", err)
	}
	if !found || fresh != 2 {
		t.Fatalf("fresh result = (%d, %t), want (2, true)", fresh, found)
	}

	close(oldRelease)
	if err := <-oldResult; err != nil {
		t.Fatalf("old GetOrLoad() error = %v", err)
	}

	cached, found, err := cache.GetOrLoad(
		context.Background(),
		"key",
		func(context.Context) (int, bool, error) { return 3, true, nil },
	)
	if err != nil {
		t.Fatalf("cached GetOrLoad() error = %v", err)
	}
	if !found || cached != 2 {
		t.Fatalf("cached result = (%d, %t), want fresh value (2, true)", cached, found)
	}
}

func TestCacheInvalidateAllPreventsInFlightLoadFromRepopulating(t *testing.T) {
	cache := newTestCache[int](t)

	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)

	go func() {
		_, _, err := cache.GetOrLoad(
			context.Background(),
			"key",
			func(context.Context) (int, bool, error) {
				close(started)
				<-release
				return 1, true, nil
			},
		)
		result <- err
	}()

	<-started
	cache.InvalidateAll()
	close(release)

	if err := <-result; err != nil {
		t.Fatalf("first GetOrLoad() error = %v", err)
	}

	var loads atomic.Int64
	value, found, err := cache.GetOrLoad(
		context.Background(),
		"key",
		func(context.Context) (int, bool, error) {
			loads.Add(1)
			return 2, true, nil
		},
	)
	if err != nil {
		t.Fatalf("second GetOrLoad() error = %v", err)
	}
	if !found || value != 2 || loads.Load() != 1 {
		t.Fatalf("second result = (%d, %t), loads=%d; want (2, true), loads=1", value, found, loads.Load())
	}
}

func TestCacheConcurrentOperations(t *testing.T) {
	cache := newTestCache[int](
		t,
		WithMaxEntries(512),
		WithSegmentCount(32),
		WithNegativeTTL(time.Minute),
	)

	const goroutines = 24
	const iterations = 300

	start := make(chan struct{})
	errorsCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for worker := range goroutines {
		go func() {
			defer wg.Done()
			<-start

			for iteration := range iterations {
				key := fmt.Sprintf("key-%d", (worker+iteration)%64)

				_, _, err := cache.GetOrLoad(
					context.Background(),
					key,
					func(context.Context) (int, bool, error) {
						return iteration, true, nil
					},
				)
				if err != nil {
					errorsCh <- err
					return
				}

				switch {
				case iteration%53 == 0:
					cache.InvalidateAll()
				case iteration%17 == 0:
					cache.Invalidate(key)
				case iteration%17 == 1:
					_ = cache.Stats()
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errorsCh)

	for err := range errorsCh {
		t.Error(err)
	}
}

func newTestCache[V any](t *testing.T, options ...Option) *Cache[V] {
	t.Helper()

	options = append(
		[]Option{
			WithMaxEntries(256),
			WithSegmentCount(8),
			WithTTL(time.Minute),
		},
		options...,
	)

	cache, err := New[V]("test", options...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Cleanup(cache.Close)

	return cache
}
