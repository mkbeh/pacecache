package pacecache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheZeroValueAndNilReceiver(t *testing.T) {
	tests := []struct {
		name  string
		cache *Cache[string, int]
	}{
		{name: "nil", cache: nil},
		{name: "zero value", cache: &Cache[string, int]{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := test.cache

			if got := cache.Name(); got != "" {
				t.Fatalf("Name() = %q, want empty string", got)
			}
			cache.Close()

			if cache.Exists("key") {
				t.Fatal("Exists() = true")
			}
			if cache.RefreshTTL("key") {
				t.Fatal("RefreshTTL() = true")
			}
			if value, found := cache.Get("key"); value != 0 || found {
				t.Fatalf("Get() = (%d, %t), want (0, false)", value, found)
			}
			if entry, found := cache.GetEntry("key"); entry != (Entry[int]{}) || found {
				t.Fatalf("GetEntry() = (%+v, %t), want zero entry and false", entry, found)
			}

			loader := func(context.Context) (int, bool, error) {
				return 1, true, nil
			}
			if value, found, err := cache.GetOrLoad(context.Background(), "key", loader); value != 0 || found || err == nil || err.Error() != "pacecache: cache is not initialized" {
				t.Fatalf("GetOrLoad() = (%d, %t, %v)", value, found, err)
			}
			if entry, found, err := cache.GetOrLoadEntry(context.Background(), "key", loader); entry != (Entry[int]{}) || found || err == nil || err.Error() != "pacecache: cache is not initialized" {
				t.Fatalf("GetOrLoadEntry() = (%+v, %t, %v)", entry, found, err)
			}

			cache.Set("key", 1, NoExpiration)
			cache.Invalidate()
			cache.Invalidate("key")
			cache.InvalidateAll()

			if got := cache.CleanupExpired(); got != 0 {
				t.Fatalf("CleanupExpired() = %d, want 0", got)
			}
			if got := cache.Stats(); got != (Stats{}) {
				t.Fatalf("Stats() = %+v, want zero Stats", got)
			}
		})
	}
}

func TestNewCacheInitializesCore(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"users",
		WithMaxEntries(64),
		WithSegmentCount(4),
		WithTTL(2*time.Minute),
		WithJitter(5*time.Second),
		WithSlidingExpiration(),
		WithCleanupBatchSize(7),
		WithCleanupEntryBudget(11),
	)

	if cache.Name() != "users" {
		t.Fatalf("Name() = %q, want %q", cache.Name(), "users")
	}
	if cache.ttl != 2*time.Minute || cache.jitter != 5*time.Second {
		t.Fatalf("cache TTL settings = (%v, %v)", cache.ttl, cache.jitter)
	}
	if cache.cleanupPolicy != (cleanupPolicy{batchSize: 7, entryBudget: 11}) {
		t.Fatalf("cleanupPolicy = %+v", cache.cleanupPolicy)
	}
	if cache.cleanup != nil {
		t.Fatal("background cleanup is enabled without WithCleanupInterval")
	}
	if len(cache.store.segments) != 4 || len(cache.states) != 4 || len(cache.stats.segments) != 4 {
		t.Fatalf("segment counts = storage %d states %d stats %d", len(cache.store.segments), len(cache.states), len(cache.stats.segments))
	}

	for index := range cache.store.segments {
		if !cache.store.segments[index].slidingExpiration {
			t.Fatalf("segment %d sliding expiration is disabled", index)
		}
		if !cache.store.segments[index].expirations.enabled() {
			t.Fatalf("segment %d expiration index is disabled", index)
		}
		if cache.states[index].group == nil {
			t.Fatalf("segment %d singleflight group = nil", index)
		}
	}

	_, err := New[string, int]("")
	if err == nil || err.Error() != "pacecache: invalid configuration: cache name must not be blank" {
		t.Fatalf("New(empty name) error = %v", err)
	}
}

func TestCacheSetGetEntryAndLRU(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"values",
		WithMaxEntries(2),
		WithSegmentCount(1),
		WithTTL(time.Minute),
	)

	cache.Set("first", 1, DefaultExpiration)
	firstInternal := cacheEntryForTest(t, cache, "first")

	entry, found := cache.GetEntry("first")
	if !found || entry.Value() != 1 {
		t.Fatalf("GetEntry(first) = (%+v, %v)", entry, found)
	}
	wantExpiresAt := cache.store.expiresAt(firstInternal.deadline)
	if !entry.ExpiresAt().Equal(wantExpiresAt) {
		t.Fatalf("ExpiresAt() = %v, want %v", entry.ExpiresAt(), wantExpiresAt)
	}

	cache.Set("second", 2, NoExpiration)
	entry, found = cache.GetEntry("second")
	if !found || entry.Value() != 2 || !entry.ExpiresAt().IsZero() {
		t.Fatalf("GetEntry(second) = (%+v, %v), want no-expiration hit", entry, found)
	}

	if value, found := cache.Get("first"); value != 1 || !found {
		t.Fatalf("Get(first) = (%d, %v)", value, found)
	}

	cache.Set("third", 3, NoExpiration)
	if _, found := cache.Get("second"); found {
		t.Fatalf("second found after eviction = %v, want false", found)
	}
	if value, found := cache.Get("first"); value != 1 || !found {
		t.Fatalf("first after eviction = (%d, %v)", value, found)
	}
	if value, found := cache.Get("third"); value != 3 || !found {
		t.Fatalf("third after insertion = (%d, %v)", value, found)
	}

	cache.Set("first", 10, NoExpiration)
	if value, found := cache.Get("first"); value != 10 || !found {
		t.Fatalf("updated first = (%d, %v), want (10, true)", value, found)
	}
	if got := cache.Stats().EvictionCount; got != 1 {
		t.Fatalf("EvictionCount = %d, want 1", got)
	}
}

func TestCacheExistsAndRefreshTTL(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"presence",
		WithMaxEntries(8),
		WithSegmentCount(1),
		WithTTL(10*time.Second),
	)

	cache.Set("expiring", 1, DefaultExpiration)
	cache.Set("forever", 2, NoExpiration)

	beforeStats := cache.Stats()
	beforeHead, beforeTail := cacheLRUEndsForTest(cache, "expiring")

	if !cache.Exists("expiring") {
		t.Fatal("Exists(expiring) = false")
	}
	if cache.Exists("missing") {
		t.Fatal("Exists(missing) = true")
	}

	afterExists := cache.Stats()
	if afterExists.HitCount != beforeStats.HitCount || afterExists.MissCount != beforeStats.MissCount {
		t.Fatalf("Exists changed lookup stats: before=%+v after=%+v", beforeStats, afterExists)
	}
	assertCacheLRUEndsForTest(t, cache, "expiring", beforeHead, beforeTail)

	index := cache.store.segmentIndex("expiring")
	segment := &cache.store.segments[index]
	segment.mu.Lock()
	item := segment.entries["expiring"]
	oldDeadline := cache.store.now() + int64(time.Second)
	segment.expirations.update(item, oldDeadline)
	segment.mu.Unlock()

	if !cache.RefreshTTL("expiring") {
		t.Fatal("RefreshTTL(expiring) = false")
	}
	refreshed := cacheEntryForTest(t, cache, "expiring")
	if refreshed.deadline <= oldDeadline {
		t.Fatalf("refreshed deadline = %d, want > %d", refreshed.deadline, oldDeadline)
	}
	if !cache.RefreshTTL("forever") {
		t.Fatal("RefreshTTL(forever) = false")
	}
	if cache.RefreshTTL("missing") {
		t.Fatal("RefreshTTL(missing) = true")
	}

	afterRefresh := cache.Stats()
	if afterRefresh.HitCount != afterExists.HitCount || afterRefresh.MissCount != afterExists.MissCount {
		t.Fatalf("RefreshTTL changed lookup stats: before=%+v after=%+v", afterExists, afterRefresh)
	}
	assertCacheLRUEndsForTest(t, cache, "expiring", beforeHead, beforeTail)

	cache.Set("expired_exists", 3, DefaultExpiration)
	cache.Set("expired_refresh", 4, DefaultExpiration)
	setCacheDeadlineForTest(t, cache, "expired_exists", cache.store.now()-1)
	setCacheDeadlineForTest(t, cache, "expired_refresh", cache.store.now()-1)

	if cache.Exists("expired_exists") {
		t.Fatal("Exists(expired) = true")
	}
	if cache.RefreshTTL("expired_refresh") {
		t.Fatal("RefreshTTL(expired) = true")
	}
	if got := cache.Stats().ExpirationCount - afterRefresh.ExpirationCount; got != 2 {
		t.Fatalf("new expiration count = %d, want 2", got)
	}
}

func TestCacheGetAndGetEntrySemantics(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"lookups",
		WithMaxEntries(8),
		WithSegmentCount(1),
		WithTTL(10*time.Second),
		WithSlidingExpiration(),
	)

	cache.Set("expiring", 42, DefaultExpiration)
	cache.Set("forever", 7, NoExpiration)

	oldDeadline := cache.store.now() + int64(time.Second)
	setCacheDeadlineForTest(t, cache, "expiring", oldDeadline)

	entry, found := cache.GetEntry("expiring")
	if !found || entry.Value() != 42 {
		t.Fatalf("GetEntry(expiring) = (%+v, %t)", entry, found)
	}
	stored := cacheEntryForTest(t, cache, "expiring")
	if stored.deadline <= oldDeadline {
		t.Fatalf("sliding deadline = %d, want > %d", stored.deadline, oldDeadline)
	}
	if !entry.ExpiresAt().Equal(cache.store.expiresAt(stored.deadline)) {
		t.Fatalf("GetEntry deadline = %v, want stored deadline %v", entry.ExpiresAt(), cache.store.expiresAt(stored.deadline))
	}

	entry, found = cache.GetEntry("forever")
	if !found || entry.Value() != 7 || !entry.ExpiresAt().IsZero() {
		t.Fatalf("GetEntry(forever) = (%+v, %t)", entry, found)
	}

	if value, found := cache.Get("expiring"); !found || value != 42 {
		t.Fatalf("Get(expiring) = (%d, %t)", value, found)
	}
	if entry, found := cache.GetEntry("missing"); entry != (Entry[int]{}) || found {
		t.Fatalf("GetEntry(missing) = (%+v, %t)", entry, found)
	}
}

func TestCacheSlidingExpirationReusesJitteredTTL(t *testing.T) {
	const (
		baseTTL = time.Hour
		jitter  = 30 * time.Minute
	)

	cache := mustNewCache[string, int](
		t,
		"sliding-jitter",
		WithMaxEntries(8),
		WithSegmentCount(1),
		WithTTL(baseTTL),
		WithJitter(jitter),
		WithSlidingExpiration(),
	)

	cache.Set("key", 42, DefaultExpiration)

	stored := cacheEntryForTest(t, cache, "key")
	effectiveTTL := stored.refreshTTL
	if effectiveTTL < baseTTL || effectiveTTL >= baseTTL+jitter {
		t.Fatalf(
			"effective TTL = %v, want in [%v, %v)",
			effectiveTTL,
			baseTTL,
			baseTTL+jitter,
		)
	}

	for iteration := range 3 {
		setCacheDeadlineForTest(
			t,
			cache,
			"key",
			cache.store.now()+int64(time.Minute),
		)

		before := cache.store.now()

		entry, found := cache.GetEntry("key")

		after := cache.store.now()
		stored = cacheEntryForTest(t, cache, "key")

		if !found || entry.Value() != 42 {
			t.Fatalf(
				"iteration %d GetEntry() = (%+v, %v)",
				iteration,
				entry,
				found,
			)
		}
		if stored.refreshTTL != effectiveTTL {
			t.Fatalf(
				"iteration %d refresh TTL = %v, want stable %v",
				iteration,
				stored.refreshTTL,
				effectiveTTL,
			)
		}

		minDeadline := deadlineAfter(before, effectiveTTL)
		maxDeadline := deadlineAfter(after, effectiveTTL)
		if stored.deadline < minDeadline || stored.deadline > maxDeadline {
			t.Fatalf(
				"iteration %d deadline = %d, want in [%d, %d]",
				iteration,
				stored.deadline,
				minDeadline,
				maxDeadline,
			)
		}
		if !entry.ExpiresAt().Equal(cache.store.expiresAt(stored.deadline)) {
			t.Fatalf(
				"iteration %d ExpiresAt() = %v, want %v",
				iteration,
				entry.ExpiresAt(),
				cache.store.expiresAt(stored.deadline),
			)
		}
	}
}

func TestCacheGetOrLoadPositive(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"positive-load",
		WithMaxEntries(32),
		WithSegmentCount(1),
		WithTTL(time.Minute),
	)

	var calls atomic.Int64
	loader := func(context.Context) (int, bool, error) {
		calls.Add(1)
		return 42, true, nil
	}

	value, found, err := cache.GetOrLoad(context.Background(), "key", loader)
	if err != nil || !found || value != 42 {
		t.Fatalf("GetOrLoad() = (%d, %v, %v)", value, found, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", calls.Load())
	}

	stored := cacheEntryForTest(t, cache, "key")
	if stored.value != 42 || stored.deadline == 0 {
		t.Fatalf("stored entry = %+v", stored)
	}

	entry, found, err := cache.GetOrLoadEntry(context.Background(), "key", func(context.Context) (int, bool, error) {
		t.Fatal("loader was called for a cached value")
		return 0, false, nil
	})
	if err != nil || !found || entry.Value() != 42 {
		t.Fatalf("GetOrLoadEntry(hit) = (%+v, %v, %v)", entry, found, err)
	}
	if !entry.ExpiresAt().Equal(cache.store.expiresAt(stored.deadline)) {
		t.Fatalf("GetOrLoadEntry ExpiresAt = %v, want %v", entry.ExpiresAt(), cache.store.expiresAt(stored.deadline))
	}

	if got := cache.Stats().LoadFoundCount; got != 1 {
		t.Fatalf("LoadFoundCount = %d, want 1", got)
	}
}

func TestCacheGetOrLoadNotFoundIsNotCached(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"not-found-load",
		WithMaxEntries(32),
		WithSegmentCount(1),
	)

	var calls atomic.Int64
	loader := func(context.Context) (int, bool, error) {
		calls.Add(1)
		return 99, false, nil
	}

	for range 2 {
		value, found, err := cache.GetOrLoad(context.Background(), "key", loader)
		if err != nil || found || value != 0 {
			t.Fatalf("GetOrLoad() = (%d, %t, %v), want (0, false, nil)", value, found, err)
		}
	}

	entry, found, err := cache.GetOrLoadEntry(context.Background(), "key", loader)
	if err != nil || found || entry != (Entry[int]{}) {
		t.Fatalf("GetOrLoadEntry() = (%+v, %t, %v), want zero miss", entry, found, err)
	}

	if calls.Load() != 3 {
		t.Fatalf("loader calls = %d, want 3", calls.Load())
	}
	if cache.Exists("key") {
		t.Fatal("not-found loader result became resident")
	}
	if got := cache.Stats().EntryCount; got != 0 {
		t.Fatalf("EntryCount = %d, want 0", got)
	}
	if got := cache.Stats().LoadNotFoundCount; got != 3 {
		t.Fatalf("LoadNotFoundCount = %d, want 3", got)
	}
}

func TestCacheGetOrLoadValidationErrorsAndCancellation(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"validation",
		WithMaxEntries(32),
		WithSegmentCount(1),
	)

	loader := func(context.Context) (int, bool, error) {
		return 1, true, nil
	}

	if _, _, err := cache.GetOrLoad(nil, "key", loader); err == nil || err.Error() != "pacecache: context is nil" {
		t.Fatalf("nil context error = %v", err)
	}
	if _, _, err := cache.GetOrLoad(context.Background(), "key", nil); err == nil || err.Error() != "pacecache: loader is nil" {
		t.Fatalf("nil loader error = %v", err)
	}

	sentinel := errors.New("load failed")
	var calls atomic.Int64
	failing := func(context.Context) (int, bool, error) {
		calls.Add(1)
		return 99, true, sentinel
	}

	value, found, err := cache.GetOrLoad(context.Background(), "error", failing)
	if value != 0 || found || !errors.Is(err, sentinel) {
		t.Fatalf("failing GetOrLoad() = (%d, %v, %v)", value, found, err)
	}
	if _, found := cache.Get("error"); found {
		t.Fatalf("error result found = %v, want false", found)
	}
	_, _, _ = cache.GetOrLoad(context.Background(), "error", failing)
	if calls.Load() != 2 {
		t.Fatalf("error loader calls = %d, want 2", calls.Load())
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var canceledCalls atomic.Int64
	if value, found, err := cache.GetOrLoad(ctx, "canceled-miss", func(context.Context) (int, bool, error) {
		canceledCalls.Add(1)
		return 1, true, nil
	}); value != 0 || found || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled miss = (%d, %v, %v)", value, found, err)
	}
	if canceledCalls.Load() != 0 {
		t.Fatalf("loader calls for canceled miss = %d, want 0", canceledCalls.Load())
	}

	cache.Set("resident", 7, NoExpiration)
	if value, found, err := cache.GetOrLoad(ctx, "resident", func(context.Context) (int, bool, error) {
		t.Fatal("loader called for resident value")
		return 0, false, nil
	}); err != nil || !found || value != 7 {
		t.Fatalf("canceled cache hit = (%d, %v, %v)", value, found, err)
	}

	stats := cache.Stats()
	if stats.LoadErrorCount != 2 {
		t.Fatalf("LoadErrorCount = %d, want 2", stats.LoadErrorCount)
	}
}

func TestCacheGetOrLoadOwnerRechecksCache(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"owner-recheck",
		WithMaxEntries(32),
		WithSegmentCount(1),
	)

	state := &cache.states[0]
	state.mu.Lock()

	var calls atomic.Int64
	done := make(chan cacheValueResult[int], 1)
	go func() {
		value, found, err := cache.GetOrLoad(context.Background(), "key", func(context.Context) (int, bool, error) {
			calls.Add(1)
			return 99, true, nil
		})
		done <- cacheValueResult[int]{value: value, found: found, err: err}
	}()

	waitForCacheCondition(t, time.Second, func() bool {
		return cache.Stats().MissCount >= 1
	})

	cache.storeValue(0, "key", 42, NoExpiration, cache.store.now())
	state.mu.Unlock()

	result := receiveCacheResult(t, done)
	if result.err != nil || !result.found || result.value != 42 {
		t.Fatalf("GetOrLoad() = (%d, %v, %v), want cached value", result.value, result.found, result.err)
	}
	if calls.Load() != 0 {
		t.Fatalf("loader calls = %d, want 0", calls.Load())
	}
}

func TestCacheGetOrLoadCoalescesMixedAPIs(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"singleflight",
		WithMaxEntries(32),
		WithSegmentCount(1),
		WithTTL(time.Minute),
	)

	const callers = 24

	start := make(chan struct{})
	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	var loaderOnce sync.Once
	var loaderCalls atomic.Int64

	loader := func(ctx context.Context) (int, bool, error) {
		loaderCalls.Add(1)

		if err := ctx.Err(); err != nil {
			return 0, false, err
		}

		loaderOnce.Do(func() {
			close(loaderStarted)
		})

		<-releaseLoader

		return 42, true, nil
	}

	contexts := make([]*observedContext, callers)

	var ready sync.WaitGroup
	ready.Add(callers)
	var done sync.WaitGroup
	done.Add(callers)
	errorsCh := make(chan error, callers)

	for index := range callers {
		contexts[index] = newObservedContext(context.Background())

		go func(index int) {
			defer done.Done()
			ready.Done()
			<-start

			if index%2 == 0 {
				value, found, err := cache.GetOrLoad(
					contexts[index],
					"key",
					loader,
				)
				if err != nil || !found || value != 42 {
					errorsCh <- fmt.Errorf(
						"GetOrLoad = (%d, %v, %v)",
						value,
						found,
						err,
					)
				}
				return
			}

			entry, found, err := cache.GetOrLoadEntry(
				contexts[index],
				"key",
				loader,
			)
			if err != nil ||
				!found ||
				entry.Value() != 42 ||
				entry.ExpiresAt().IsZero() {
				errorsCh <- fmt.Errorf(
					"GetOrLoadEntry = (%+v, %v, %v)",
					entry,
					found,
					err,
				)
			}
		}(index)
	}

	ready.Wait()
	close(start)
	receiveCacheSignal(t, loaderStarted)

	// Done is first observed after DoChan has registered the caller and
	// getOrLoad has reached its result wait. Waiting for every observation
	// makes the expected shared-wave statistics deterministic.
	for _, ctx := range contexts {
		receiveCacheSignal(t, ctx.observed)
	}

	close(releaseLoader)

	done.Wait()
	close(errorsCh)

	for err := range errorsCh {
		t.Error(err)
	}
	if loaderCalls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", loaderCalls.Load())
	}

	stats := cache.Stats()
	if stats.LoadFoundCount != 1 {
		t.Fatalf("LoadFoundCount = %d, want 1", stats.LoadFoundCount)
	}
	if stats.SharedCount != callers {
		t.Fatalf("SharedCount = %d, want %d", stats.SharedCount, callers)
	}
}

func TestCacheGetOrLoadWaiterCancellation(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"waiter-cancellation",
		WithMaxEntries(32),
		WithSegmentCount(1),
	)

	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	ownerDone := make(chan cacheValueResult[int], 1)

	go func() {
		value, found, err := cache.GetOrLoad(
			context.Background(),
			"key",
			func(context.Context) (int, bool, error) {
				close(loaderStarted)
				<-releaseLoader
				return 42, true, nil
			},
		)
		ownerDone <- cacheValueResult[int]{
			value: value,
			found: found,
			err:   err,
		}
	}()

	receiveCacheSignal(t, loaderStarted)

	parent, cancel := context.WithCancel(context.Background())
	ctx := newObservedContext(parent)
	waiterDone := make(chan error, 1)

	go func() {
		_, _, err := cache.GetOrLoad(
			ctx,
			"key",
			func(context.Context) (int, bool, error) {
				return 99, true, nil
			},
		)
		waiterDone <- err
	}()

	// Done is observed only after the waiter has joined the existing wave and
	// reached the result wait, so cancellation cannot accidentally exercise
	// the pre-registration fast-fail path.
	receiveCacheSignal(t, ctx.observed)
	cancel()

	if err := receiveCacheResult(t, waiterDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error = %v, want context.Canceled", err)
	}

	close(releaseLoader)

	owner := receiveCacheResult(t, ownerDone)
	if owner.err != nil || !owner.found || owner.value != 42 {
		t.Fatalf(
			"owner result = (%d, %v, %v)",
			owner.value,
			owner.found,
			owner.err,
		)
	}
	if value, found := cache.Get("key"); value != 42 || !found {
		t.Fatalf(
			"cached value = (%d, %v), want (42, true)",
			value,
			found,
		)
	}
}

func TestCacheGetOrLoadPublicationBarriers(t *testing.T) {
	tests := []struct {
		name        string
		found       bool
		mutate      func(*Cache[string, int])
		verifyCache func(*testing.T, *Cache[string, int])
	}{
		{
			name:  "set supersedes positive",
			found: true,
			mutate: func(cache *Cache[string, int]) {
				cache.Set("key", 99, NoExpiration)
			},
			verifyCache: func(t *testing.T, cache *Cache[string, int]) {
				if value, found := cache.Get("key"); value != 99 || !found {
					t.Fatalf("cache after Set = (%d, %v)", value, found)
				}
			},
		},
		{
			name:  "invalidate supersedes positive",
			found: true,
			mutate: func(cache *Cache[string, int]) {
				cache.Invalidate("key")
			},
			verifyCache: expectCacheMiss,
		},
		{
			name:  "invalidate all supersedes positive",
			found: true,
			mutate: func(cache *Cache[string, int]) {
				cache.InvalidateAll()
			},
			verifyCache: expectCacheMiss,
		},
		{
			name:  "set supersedes negative",
			found: false,
			mutate: func(cache *Cache[string, int]) {
				cache.Set("key", 99, NoExpiration)
			},
			verifyCache: func(t *testing.T, cache *Cache[string, int]) {
				if value, found := cache.Get("key"); value != 99 || !found {
					t.Fatalf("cache after Set = (%d, %v)", value, found)
				}
			},
		},
		{
			name:  "invalidate supersedes negative",
			found: false,
			mutate: func(cache *Cache[string, int]) {
				cache.Invalidate("key")
			},
			verifyCache: expectCacheMiss,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := mustNewCache[string, int](
				t,
				"barrier",
				WithMaxEntries(32),
				WithSegmentCount(1),
			)

			started := make(chan struct{})
			release := make(chan struct{})
			done := make(chan cacheEntryResult[int], 1)

			go func() {
				entry, found, err := cache.GetOrLoadEntry(context.Background(), "key", func(context.Context) (int, bool, error) {
					close(started)
					<-release
					return 42, test.found, nil
				})
				done <- cacheEntryResult[int]{entry: entry, found: found, err: err}
			}()

			receiveCacheSignal(t, started)
			test.mutate(cache)
			close(release)

			result := receiveCacheResult(t, done)
			if result.entry != (Entry[int]{}) || result.found || !errors.Is(result.err, ErrLoadSuperseded) {
				t.Fatalf("GetOrLoadEntry() = (%+v, %v, %v), want superseded zero result", result.entry, result.found, result.err)
			}
			test.verifyCache(t, cache)

			stats := cache.Stats()
			if stats.LoadSupersededCount != 1 {
				t.Fatalf("LoadSupersededCount = %d, want 1", stats.LoadSupersededCount)
			}
			if test.found && stats.LoadFoundCount != 1 {
				t.Fatalf("LoadFoundCount = %d, want 1", stats.LoadFoundCount)
			}
			if !test.found && stats.LoadNotFoundCount != 1 {
				t.Fatalf("LoadNotFoundCount = %d, want 1", stats.LoadNotFoundCount)
			}
			if stats.LoadErrorCount != 0 {
				t.Fatalf("LoadErrorCount = %d, want 0", stats.LoadErrorCount)
			}
		})
	}
}

func TestCacheLoadSupersededCountCountsSharedWave(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"superseded-wave",
		WithMaxEntries(32),
		WithSegmentCount(1),
	)

	const callers = 16

	start := make(chan struct{})
	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	var loaderOnce sync.Once
	var loaderCalls atomic.Int64

	loader := func(ctx context.Context) (int, bool, error) {
		loaderCalls.Add(1)

		if err := ctx.Err(); err != nil {
			return 0, false, err
		}

		loaderOnce.Do(func() {
			close(loaderStarted)
		})

		<-releaseLoader

		return 42, true, nil
	}

	contexts := make([]*observedContext, callers)
	results := make(chan cacheValueResult[int], callers)

	var ready sync.WaitGroup
	ready.Add(callers)
	var done sync.WaitGroup
	done.Add(callers)

	for index := range callers {
		contexts[index] = newObservedContext(context.Background())

		go func(index int) {
			defer done.Done()
			ready.Done()
			<-start

			value, found, err := cache.GetOrLoad(
				contexts[index],
				"key",
				loader,
			)
			results <- cacheValueResult[int]{
				value: value,
				found: found,
				err:   err,
			}
		}(index)
	}

	ready.Wait()
	close(start)
	receiveCacheSignal(t, loaderStarted)

	for _, ctx := range contexts {
		receiveCacheSignal(t, ctx.observed)
	}

	cache.Set("key", 99, NoExpiration)
	close(releaseLoader)

	done.Wait()
	close(results)

	for result := range results {
		if result.value != 0 ||
			result.found ||
			!errors.Is(result.err, ErrLoadSuperseded) {
			t.Fatalf(
				"GetOrLoad() = (%d, %v, %v), want superseded zero result",
				result.value,
				result.found,
				result.err,
			)
		}
	}

	if loaderCalls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", loaderCalls.Load())
	}
	if value, found := cache.Get("key"); value != 99 || !found {
		t.Fatalf("cache after Set = (%d, %v), want (99, true)", value, found)
	}

	stats := cache.Stats()
	if stats.LoadFoundCount != 1 {
		t.Fatalf("LoadFoundCount = %d, want 1", stats.LoadFoundCount)
	}
	if stats.LoadSupersededCount != 1 {
		t.Fatalf(
			"LoadSupersededCount = %d, want one superseded wave",
			stats.LoadSupersededCount,
		)
	}
	if stats.SharedCount != callers {
		t.Fatalf("SharedCount = %d, want %d", stats.SharedCount, callers)
	}
}

func TestCacheInvalidateAllSupersedesMultipleActiveKeys(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"invalidate-all-loads",
		WithMaxEntries(32),
		WithSegmentCount(1),
	)

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	release := make(chan struct{})

	firstDone := make(chan cacheValueResult[int], 1)
	secondDone := make(chan cacheValueResult[int], 1)

	go func() {
		value, found, err := cache.GetOrLoad(
			context.Background(),
			"first",
			func(context.Context) (int, bool, error) {
				close(firstStarted)
				<-release
				return 1, true, nil
			},
		)
		firstDone <- cacheValueResult[int]{
			value: value,
			found: found,
			err:   err,
		}
	}()

	go func() {
		value, found, err := cache.GetOrLoad(
			context.Background(),
			"second",
			func(context.Context) (int, bool, error) {
				close(secondStarted)
				<-release
				return 2, true, nil
			},
		)
		secondDone <- cacheValueResult[int]{
			value: value,
			found: found,
			err:   err,
		}
	}()

	receiveCacheSignal(t, firstStarted)
	receiveCacheSignal(t, secondStarted)

	cache.InvalidateAll()
	close(release)

	for name, result := range map[string]cacheValueResult[int]{
		"first":  receiveCacheResult(t, firstDone),
		"second": receiveCacheResult(t, secondDone),
	} {
		if result.value != 0 ||
			result.found ||
			!errors.Is(result.err, ErrLoadSuperseded) {
			t.Fatalf(
				"%s GetOrLoad() = (%d, %v, %v), want superseded zero result",
				name,
				result.value,
				result.found,
				result.err,
			)
		}
	}

	if _, found := cache.Get("first"); found {
		t.Fatalf("first found = %v, want false", found)
	}
	if _, found := cache.Get("second"); found {
		t.Fatalf("second found = %v, want false", found)
	}

	stats := cache.Stats()
	if stats.LoadFoundCount != 2 {
		t.Fatalf("LoadFoundCount = %d, want 2", stats.LoadFoundCount)
	}
	if stats.LoadSupersededCount != 2 {
		t.Fatalf("LoadSupersededCount = %d, want 2", stats.LoadSupersededCount)
	}
	if stats.LoadErrorCount != 0 {
		t.Fatalf("LoadErrorCount = %d, want 0", stats.LoadErrorCount)
	}
}

func TestCacheLoaderErrorPrecedesSupersededError(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"loader-error",
		WithMaxEntries(32),
		WithSegmentCount(1),
	)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	sentinel := errors.New("loader failed")

	go func() {
		_, _, err := cache.GetOrLoad(context.Background(), "key", func(context.Context) (int, bool, error) {
			close(started)
			<-release
			return 0, false, sentinel
		})
		done <- err
	}()

	receiveCacheSignal(t, started)
	cache.Invalidate("key")
	close(release)

	if err := receiveCacheResult(t, done); !errors.Is(err, sentinel) || errors.Is(err, ErrLoadSuperseded) {
		t.Fatalf("GetOrLoad() error = %v, want loader error", err)
	}

	stats := cache.Stats()
	if stats.LoadErrorCount != 1 || stats.LoadSupersededCount != 0 {
		t.Fatalf("load stats = errors %d superseded %d, want 1 and 0", stats.LoadErrorCount, stats.LoadSupersededCount)
	}
}

func TestCacheMutationOfOtherKeyDoesNotSupersedeLoad(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Cache[string, int])
	}{
		{
			name: "set",
			mutate: func(cache *Cache[string, int]) {
				cache.Set("other", 99, NoExpiration)
			},
		},
		{
			name: "invalidate",
			mutate: func(cache *Cache[string, int]) {
				cache.Set("other", 99, NoExpiration)
				cache.Invalidate("other")
			},
		},
		{
			name: "batch invalidate",
			mutate: func(cache *Cache[string, int]) {
				cache.Set("other", 99, NoExpiration)
				cache.Invalidate("other", "missing")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := mustNewCache[string, int](
				t,
				"other-key",
				WithMaxEntries(32),
				WithSegmentCount(1),
			)

			started := make(chan struct{})
			release := make(chan struct{})
			done := make(chan cacheValueResult[int], 1)

			go func() {
				value, found, err := cache.GetOrLoad(context.Background(), "loading", func(context.Context) (int, bool, error) {
					close(started)
					<-release
					return 42, true, nil
				})
				done <- cacheValueResult[int]{value: value, found: found, err: err}
			}()

			receiveCacheSignal(t, started)
			test.mutate(cache)
			close(release)

			result := receiveCacheResult(t, done)
			if result.err != nil || !result.found || result.value != 42 {
				t.Fatalf("GetOrLoad() = (%d, %v, %v)", result.value, result.found, result.err)
			}
			if value, found := cache.Get("loading"); value != 42 || !found {
				t.Fatalf("published value = (%d, %v)", value, found)
			}
			if got := cache.Stats().LoadSupersededCount; got != 0 {
				t.Fatalf("LoadSupersededCount = %d, want 0", got)
			}
		})
	}
}

func TestCacheInvalidate(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"invalidate",
		WithMaxEntries(64),
		WithSegmentCount(4),
	)

	for index, key := range []string{"first", "second", "third"} {
		cache.Set(key, index+1, NoExpiration)
	}

	cache.Invalidate("first", "second", "second", "missing")

	if _, found := cache.Get("first"); found {
		t.Fatalf("first found = %v, want false", found)
	}
	if _, found := cache.Get("second"); found {
		t.Fatalf("second found = %v, want false", found)
	}
	if value, found := cache.Get("third"); value != 3 || !found {
		t.Fatalf("third = (%d, %v), want resident", value, found)
	}
	if got := cache.Stats().InvalidatedKeyCount; got != 2 {
		t.Fatalf("InvalidatedKeyCount = %d, want 2", got)
	}

	cache.Invalidate()
	if got := cache.Stats().InvalidatedKeyCount; got != 2 {
		t.Fatalf("InvalidatedKeyCount after no-op = %d, want 2", got)
	}
}

func TestCacheConcurrentBatchInvalidationLockOrder(t *testing.T) {
	cache := mustNewCache[string, int](
		t,
		"lock-order",
		WithMaxEntries(64),
		WithSegmentCount(4),
	)
	first, second := cacheKeysInDifferentSegmentsForTest(t, cache)

	for iteration := 0; iteration < 50; iteration++ {
		cache.Set(first, 1, NoExpiration)
		cache.Set(second, 2, NoExpiration)

		start := make(chan struct{})
		done := make(chan struct{}, 2)

		go func() {
			<-start
			cache.Invalidate(first, second)
			done <- struct{}{}
		}()
		go func() {
			<-start
			cache.Invalidate(second, first)
			done <- struct{}{}
		}()

		close(start)
		receiveCacheSignal(t, done)
		receiveCacheSignal(t, done)
	}
}

func TestCacheInvalidateAllAndCleanupExpired(t *testing.T) {
	t.Run("invalidate all counts resident entries", func(t *testing.T) {
		cache := mustNewCache[string, int](
			t,
			"invalidate-all",
			WithMaxEntries(32),
			WithSegmentCount(2),
		)

		cache.Set("first", 1, NoExpiration)
		cache.Set("second", 2, NoExpiration)
		cache.Set("expired", 3, time.Minute)
		setCacheDeadlineForTest(t, cache, "expired", cache.store.now()-1)

		cache.InvalidateAll()

		if got := cache.Stats().InvalidatedAllCount; got != 3 {
			t.Fatalf("InvalidatedAllCount = %d, want 3", got)
		}
		if got := cache.Stats().EntryCount; got != 0 {
			t.Fatalf("EntryCount = %d, want 0", got)
		}
	})

	t.Run("manual cleanup", func(t *testing.T) {
		store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
		cache := &Cache[string, int]{
			name:   "cleanup",
			store:  store,
			states: newCacheStates[string, int](1),
			stats:  newStatsCollector(1),
			cleanupPolicy: cleanupPolicy{
				batchSize:   1,
				entryBudget: 1,
			},
		}

		store.setAt(0, "first", 1, 0, 1, cache.stats.segment(0))
		store.setAt(0, "second", 2, 0, 2, cache.stats.segment(0))
		store.setAt(0, "live", 3, 0, 1<<60, cache.stats.segment(0))

		if got := cache.CleanupExpired(); got != 2 {
			t.Fatalf("CleanupExpired() = %d, want 2", got)
		}
		if got := cache.Stats().EntryCount; got != 1 {
			t.Fatalf("EntryCount = %d, want 1", got)
		}
		stats := cache.Stats()
		if stats.ExpirationCount != 2 {
			t.Fatalf("ExpirationCount = %d, want 2", stats.ExpirationCount)
		}
		if stats.CleanupCount != 1 {
			t.Fatalf("CleanupCount = %d, want 1", stats.CleanupCount)
		}

		if got := cache.CleanupExpired(); got != 0 {
			t.Fatalf("second CleanupExpired() = %d, want 0", got)
		}
		if got := cache.Stats().CleanupCount; got != 2 {
			t.Fatalf("CleanupCount after second call = %d, want 2", got)
		}
	})
}

func TestCacheSupportsComparableKeyTypes(t *testing.T) {
	integers := mustNewCache[int64, string](
		t,
		"integers",
		WithMaxEntries(32),
		WithSegmentCount(4),
	)
	integers.Set(42, "value", NoExpiration)
	if value, found := integers.Get(42); value != "value" || !found {
		t.Fatalf("int64 key Get() = (%q, %v)", value, found)
	}

	type compositeKey struct {
		Tenant string
		ID     int
	}

	composite := mustNewCache[compositeKey, int](
		t,
		"composite",
		WithMaxEntries(32),
		WithSegmentCount(4),
	)

	stored := compositeKey{Tenant: "acme", ID: 7}
	equal := compositeKey{Tenant: "acme", ID: 7}

	var calls atomic.Int64
	value, found, err := composite.GetOrLoad(
		context.Background(),
		stored,
		func(context.Context) (int, bool, error) {
			calls.Add(1)
			return 99, true, nil
		},
	)
	if err != nil || !found || value != 99 {
		t.Fatalf(
			"composite key GetOrLoad() = (%d, %v, %v)",
			value,
			found,
			err,
		)
	}

	if value, found := composite.Get(equal); value != 99 || !found {
		t.Fatalf("Get(equal key) = (%d, %v)", value, found)
	}
	if !composite.Exists(equal) {
		t.Fatal("Exists(equal key) = false")
	}

	value, found, err = composite.GetOrLoad(
		context.Background(),
		equal,
		func(context.Context) (int, bool, error) {
			calls.Add(1)
			return 100, true, nil
		},
	)
	if err != nil || !found || value != 99 {
		t.Fatalf(
			"GetOrLoad(equal key) = (%d, %v, %v)",
			value,
			found,
			err,
		)
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", calls.Load())
	}

	composite.Invalidate(equal)
	if _, found := composite.Get(stored); found {
		t.Fatalf(
			"Get(stored key) after Invalidate(equal) = %v, want false",
			found,
		)
	}
}

func TestCacheInternalHelpers(t *testing.T) {
	t.Run("new cache states", func(t *testing.T) {
		states := newCacheStates[string, int](3)
		if len(states) != 3 {
			t.Fatalf("states = %d, want 3", len(states))
		}
		for index := range states {
			if states[index].group == nil {
				t.Fatalf("state %d group = nil", index)
			}
		}
	})

	t.Run("deadline after", func(t *testing.T) {
		if got := deadlineAfter(10, 0); got != 0 {
			t.Fatalf("deadlineAfter zero = %d, want 0", got)
		}
		if got := deadlineAfter(10, NoExpiration); got != 0 {
			t.Fatalf("deadlineAfter NoExpiration = %d, want 0", got)
		}
		if got := deadlineAfter(10, 5); got != 15 {
			t.Fatalf("deadlineAfter normal = %d, want 15", got)
		}
		if got := deadlineAfter(int64(maxDuration)-2, 5); got != int64(maxDuration) {
			t.Fatalf("deadlineAfter overflow = %d, want maxDuration", got)
		}
	})

	t.Run("jittered ttl", func(t *testing.T) {
		if got := jitteredTTL(time.Second, 0); got != time.Second {
			t.Fatalf("jitteredTTL without jitter = %v", got)
		}
		if got := jitteredTTL(maxDuration, time.Second); got != maxDuration {
			t.Fatalf("jitteredTTL at max = %v, want maxDuration", got)
		}

		for range 100 {
			got := jitteredTTL(time.Second, 100*time.Millisecond)
			if got < time.Second || got >= 1100*time.Millisecond {
				t.Fatalf("jitteredTTL = %v, out of range", got)
			}
		}
	})

	t.Run("effective expiration ttl", func(t *testing.T) {
		cache := &Cache[string, int]{ttl: time.Second}
		if got := cache.effectiveExpirationTTL(DefaultExpiration); got != time.Second {
			t.Fatalf("default effective TTL = %v, want %v", got, time.Second)
		}
		if got := cache.effectiveExpirationTTL(2 * time.Second); got != 2*time.Second {
			t.Fatalf("custom effective TTL = %v", got)
		}
		if got := cache.effectiveExpirationTTL(NoExpiration); got != 0 {
			t.Fatalf("NoExpiration effective TTL = %v, want 0", got)
		}
	})
}

type observedContext struct {
	context.Context

	once     sync.Once
	observed chan struct{}
}

func newObservedContext(parent context.Context) *observedContext {
	return &observedContext{
		Context:  parent,
		observed: make(chan struct{}),
	}
}

func (ctx *observedContext) Done() <-chan struct{} {
	ctx.once.Do(func() {
		close(ctx.observed)
	})

	return ctx.Context.Done()
}

type cacheValueResult[V any] struct {
	value V
	found bool
	err   error
}

type cacheEntryResult[V any] struct {
	entry Entry[V]
	found bool
	err   error
}

func mustNewCache[K comparable, V any](
	t *testing.T,
	name string,
	options ...Option,
) *Cache[K, V] {
	t.Helper()

	cache, err := New[K, V](name, options...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(cache.Close)

	return cache
}

func receiveCacheResult[T any](t *testing.T, channel <-chan T) T {
	t.Helper()

	select {
	case result := <-channel:
		return result
	case <-time.After(2 * time.Second):
		var zero T
		t.Fatal("timed out waiting for result")
		return zero
	}
}

func receiveCacheSignal(t *testing.T, channel <-chan struct{}) {
	t.Helper()

	select {
	case <-channel:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func waitForCacheCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		runtime.Gosched()
	}

	t.Fatal("condition was not satisfied before timeout")
}

func cacheEntryForTest[K comparable, V any](
	t *testing.T,
	cache *Cache[K, V],
	key K,
) entry[K, V] {
	t.Helper()

	index := cache.store.segmentIndex(key)
	segment := &cache.store.segments[index]

	segment.mu.Lock()
	defer segment.mu.Unlock()

	item := segment.entries[key]
	if item == nil {
		t.Fatalf("entry %v is not resident", key)
	}

	return *item
}

func setCacheDeadlineForTest[K comparable, V any](
	t *testing.T,
	cache *Cache[K, V],
	key K,
	deadline int64,
) {
	t.Helper()

	index := cache.store.segmentIndex(key)
	segment := &cache.store.segments[index]

	segment.mu.Lock()
	defer segment.mu.Unlock()

	item := segment.entries[key]
	if item == nil {
		t.Fatalf("entry %v is not resident", key)
	}

	segment.expirations.update(item, deadline)
}

func cacheLRUEndsForTest[V any](
	cache *Cache[string, V],
	key string,
) (head, tail string) {
	index := cache.store.segmentIndex(key)
	segment := &cache.store.segments[index]

	segment.mu.Lock()
	defer segment.mu.Unlock()

	if segment.head != nil {
		head = segment.head.key
	}
	if segment.tail != nil {
		tail = segment.tail.key
	}

	return head, tail
}

func assertCacheLRUEndsForTest[V any](
	t *testing.T,
	cache *Cache[string, V],
	key string,
	wantHead string,
	wantTail string,
) {
	t.Helper()

	head, tail := cacheLRUEndsForTest(cache, key)
	if head != wantHead || tail != wantTail {
		t.Fatalf("LRU ends = (%q, %q), want (%q, %q)", head, tail, wantHead, wantTail)
	}
}

func expectCacheMiss(t *testing.T, cache *Cache[string, int]) {
	t.Helper()

	if value, found := cache.Get("key"); value != 0 || found {
		t.Fatalf("cache state = (%d, %v), want miss", value, found)
	}
}

func cacheKeysInDifferentSegmentsForTest[V any](
	t *testing.T,
	cache *Cache[string, V],
) (string, string) {
	t.Helper()

	first := "key-0"
	firstIndex := cache.store.segmentIndex(first)

	for index := 1; index < 10_000; index++ {
		candidate := fmt.Sprintf("key-%d", index)
		if cache.store.segmentIndex(candidate) != firstIndex {
			return first, candidate
		}
	}

	t.Fatal("could not find keys in different segments")
	return "", ""
}
