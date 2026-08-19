package pacecache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCacheSetOverwritesExistingValue(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(4), WithSegmentCount(1))

	cache.Set("key", 1, time.Minute)
	cache.Set("key", 2, NoExpiration)

	value, found := cache.Get("key")
	if value != 2 || !found {
		t.Fatalf("Get(key) = (%d, %t), want (2, true)", value, found)
	}
	if got := cache.Stats().EntryCount; got != 1 {
		t.Fatalf("EntryCount = %d, want 1", got)
	}
}

func TestCacheGetAndInvalidate(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(8), WithSegmentCount(1))
	cache.Set("key", 42, NoExpiration)

	before := cache.Stats()

	value, found := cache.GetAndInvalidate("key")
	if !found || value != 42 {
		t.Fatalf("GetAndInvalidate(key) = (%d, %t), want (42, true)", value, found)
	}
	if cache.Exists("key") {
		t.Fatal("key remains cached after GetAndInvalidate")
	}

	after := cache.Stats()
	if after.InvalidatedKeyCount != before.InvalidatedKeyCount+1 {
		t.Fatalf("InvalidatedKeyCount = %d, want %d", after.InvalidatedKeyCount, before.InvalidatedKeyCount+1)
	}
	if after.HitCount != before.HitCount || after.MissCount != before.MissCount {
		t.Fatalf("GetAndInvalidate changed lookup stats: hits=%d/%d misses=%d/%d", before.HitCount, after.HitCount, before.MissCount, after.MissCount)
	}

	value, found = cache.GetAndInvalidate("missing")
	if found || value != 0 {
		t.Fatalf("GetAndInvalidate(missing) = (%d, %t), want (0, false)", value, found)
	}
	if got := cache.Stats().InvalidatedKeyCount; got != after.InvalidatedKeyCount {
		t.Fatalf("missing key changed InvalidatedKeyCount to %d", got)
	}
}

func TestCacheGetAndInvalidateIsAtomic(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(8), WithSegmentCount(1))
	cache.Set("key", 42, NoExpiration)

	const callers = 32

	type result struct {
		value int
		found bool
	}

	start := make(chan struct{})
	results := make(chan result, callers)

	var group sync.WaitGroup
	group.Add(callers)

	for range callers {
		go func() {
			defer group.Done()
			<-start

			value, found := cache.GetAndInvalidate("key")
			results <- result{value: value, found: found}
		}()
	}

	close(start)
	waitTestGroup(t, &group)
	close(results)

	foundCount := 0
	for current := range results {
		if current.found {
			foundCount++
			if current.value != 42 {
				t.Fatalf("winning GetAndInvalidate() value = %d, want 42", current.value)
			}
			continue
		}

		if current.value != 0 {
			t.Fatalf("losing GetAndInvalidate() value = %d, want zero", current.value)
		}
	}

	if foundCount != 1 {
		t.Fatalf("successful GetAndInvalidate() calls = %d, want 1", foundCount)
	}
	if got := cache.Stats().InvalidatedKeyCount; got != 1 {
		t.Fatalf("InvalidatedKeyCount = %d, want 1", got)
	}
}

func TestCacheGetAndInvalidateExpiredEntry(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(8), WithSegmentCount(1))
	index := cache.store.segmentIndex("expired")

	cache.store.setAt(
		index,
		"expired",
		7,
		time.Nanosecond,
		-1,
		cache.stats.segment(index),
	)

	before := cache.Stats()

	value, found := cache.GetAndInvalidate("expired")
	if found || value != 0 {
		t.Fatalf("GetAndInvalidate(expired) = (%d, %t), want (0, false)", value, found)
	}
	if cache.Exists("expired") {
		t.Fatal("expired entry remains resident")
	}

	after := cache.Stats()
	if after.ExpirationCount != before.ExpirationCount+1 {
		t.Fatalf("ExpirationCount = %d, want %d", after.ExpirationCount, before.ExpirationCount+1)
	}
	if after.InvalidatedKeyCount != before.InvalidatedKeyCount {
		t.Fatalf("expired entry changed InvalidatedKeyCount to %d", after.InvalidatedKeyCount)
	}
}

func TestCacheGetAndInvalidateSupersedesInflightLoad(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(8), WithSegmentCount(1))

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

				return 42, true, nil
			},
		)
		result <- err
	}()

	waitTestSignal(t, started)

	value, found := cache.GetAndInvalidate("key")
	if found || value != 0 {
		t.Fatalf("GetAndInvalidate(key) during load = (%d, %t), want (0, false)", value, found)
	}

	close(release)

	if err := receiveTestValue(t, result); !errors.Is(err, ErrLoadSuperseded) {
		t.Fatalf("GetOrLoad() error = %v, want ErrLoadSuperseded", err)
	}
	if cache.Exists("key") {
		t.Fatal("superseded loader repopulated key")
	}

	stats := cache.Stats()
	if stats.LoadSupersededCount != 1 {
		t.Fatalf("LoadSupersededCount = %d, want 1", stats.LoadSupersededCount)
	}
	if stats.InvalidatedKeyCount != 0 {
		t.Fatalf("InvalidatedKeyCount = %d, want 0 for missing resident key", stats.InvalidatedKeyCount)
	}
}

func TestZeroValueCacheGetAndInvalidate(t *testing.T) {
	var cache Cache[string, int]

	value, found := cache.GetAndInvalidate("key")
	if found || value != 0 {
		t.Fatalf("GetAndInvalidate() = (%d, %t), want (0, false)", value, found)
	}
}

func TestInvalidateMultipleKeysAndDuplicates(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(64), WithSegmentCount(4))
	cache.Set("a", 1, NoExpiration)
	cache.Set("b", 2, NoExpiration)
	cache.Set("c", 3, NoExpiration)

	cache.Invalidate("a", "b", "a", "missing")

	if _, found := cache.Get("a"); found {
		t.Fatal("a remains cached")
	}
	if _, found := cache.Get("b"); found {
		t.Fatal("b remains cached")
	}
	if value, found := cache.Get("c"); value != 3 || !found {
		t.Fatalf("c = (%d, %t), want retained hit", value, found)
	}
	if got := cache.Stats().InvalidatedKeyCount; got != 2 {
		t.Fatalf("InvalidatedKeyCount = %d, want 2", got)
	}
}

func TestConcurrentMultiKeyInvalidationLockOrderDoesNotDeadlock(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(16), WithSegmentCount(8))
	cache.Set("a", 1, NoExpiration)
	cache.Set("b", 2, NoExpiration)

	var group sync.WaitGroup
	group.Add(2)
	for _, keys := range [][]string{{"a", "b"}, {"b", "a"}} {
		go func() {
			defer group.Done()
			for range 1_000 {
				cache.Invalidate(keys...)
			}
		}()
	}

	waitTestGroup(t, &group)
}

func TestCacheStoresTTLPolicyForDefaultCustomAndNoExpiration(t *testing.T) {
	cache := mustNewCache[int](
		t,
		"users",
		WithMaxEntries(8),
		WithSegmentCount(1),
		WithTTL(10*time.Second),
		WithJitter(time.Second),
		WithSlidingExpiration(),
	)

	cache.Set("default", 1, DefaultExpiration)
	defaultItem := cache.store.segments[0].entries["default"]
	if defaultItem.refreshTTL < 10*time.Second || defaultItem.refreshTTL >= 11*time.Second || defaultItem.deadline == 0 {
		t.Fatalf("default entry refreshTTL/deadline = %v/%d", defaultItem.refreshTTL, defaultItem.deadline)
	}
	selectedTTL := defaultItem.refreshTTL
	cache.Get("default")
	if defaultItem.refreshTTL != selectedTTL {
		t.Fatalf("sliding read changed selected jitter: %v -> %v", selectedTTL, defaultItem.refreshTTL)
	}

	cache.Set("custom", 2, 3*time.Second)
	customItem := cache.store.segments[0].entries["custom"]
	if customItem.refreshTTL < 3*time.Second || customItem.refreshTTL >= 4*time.Second {
		t.Fatalf("custom refreshTTL = %v, want [3s,4s)", customItem.refreshTTL)
	}

	cache.Set("forever", 3, NoExpiration)
	forever := cache.store.segments[0].entries["forever"]
	if forever.refreshTTL != 0 || forever.deadline != 0 {
		t.Fatalf("NoExpiration entry = refreshTTL:%v deadline:%d, want 0/0", forever.refreshTTL, forever.deadline)
	}
}

func TestCacheCleanupExpired(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
	cache := &Cache[string, int]{
		name:   "test",
		store:  store,
		states: newCacheStates[string, int](1),
		stats:  newStatsCollector(1),
		cleanupPolicy: cleanupPolicy{
			batchSize:   defaultCleanupBatchSize,
			entryBudget: defaultCleanupEntryBudget,
		},
		ttl: time.Minute,
	}

	store.setAt(0, "expired", 1, time.Nanosecond, -1, cache.stats.segment(0))
	store.setAt(0, "live", 2, time.Hour, int64(time.Hour), cache.stats.segment(0))

	if removed := cache.CleanupExpired(); removed != 1 {
		t.Fatalf("CleanupExpired() = %d, want 1", removed)
	}
	if _, ok := store.segments[0].entries["expired"]; ok {
		t.Fatal("expired entry remains resident")
	}
	if _, ok := store.segments[0].entries["live"]; !ok {
		t.Fatal("live entry was removed")
	}
	if cache.Stats().CleanupCount != 1 {
		t.Fatalf("CleanupCount = %d, want 1", cache.Stats().CleanupCount)
	}
}

func TestCleanupExpiredDetectsStorageIndexMismatch(t *testing.T) {
	segment := storageSegment[string, int]{
		entries:     make(map[string]*entry[string, int]),
		expirations: newExpirationIndex[string, int](time.Nanosecond),
		maxEntries:  1,
	}
	item := &entry[string, int]{key: "orphan"}
	segment.expirations.update(item, 1)

	requirePanic(t, func() {
		segment.cleanupExpired(10, 1, nil)
	})
}
