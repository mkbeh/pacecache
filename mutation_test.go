package pacecache

import (
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
