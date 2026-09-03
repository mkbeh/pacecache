package pacecache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCacheGetAndDelete(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(8), WithSegmentCount(1))
	cache.Set("key", 42, NoExpiration)

	before := cache.Stats()

	value, found := cache.GetAndDelete("key")
	if !found || value != 42 {
		t.Fatalf("GetAndDelete(key) = (%d, %t), want (42, true)", value, found)
	}
	if cache.Exists("key") {
		t.Fatal("key remains cached after GetAndDelete")
	}

	after := cache.Stats()
	if after.DeletedEntryCount != before.DeletedEntryCount+1 {
		t.Fatalf("DeletedEntryCount = %d, want %d", after.DeletedEntryCount, before.DeletedEntryCount+1)
	}
	if after.HitCount != before.HitCount || after.MissCount != before.MissCount {
		t.Fatalf("GetAndDelete changed lookup stats: hits=%d/%d misses=%d/%d", before.HitCount, after.HitCount, before.MissCount, after.MissCount)
	}

	value, found = cache.GetAndDelete("missing")
	if found || value != 0 {
		t.Fatalf("GetAndDelete(missing) = (%d, %t), want (0, false)", value, found)
	}
	if got := cache.Stats().DeletedEntryCount; got != after.DeletedEntryCount {
		t.Fatalf("missing key changed DeletedEntryCount to %d", got)
	}
}

func TestCacheGetAndDeleteIsAtomic(t *testing.T) {
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

			value, found := cache.GetAndDelete("key")
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
				t.Fatalf("winning GetAndDelete() value = %d, want 42", current.value)
			}
			continue
		}

		if current.value != 0 {
			t.Fatalf("losing GetAndDelete() value = %d, want zero", current.value)
		}
	}

	if foundCount != 1 {
		t.Fatalf("successful GetAndDelete() calls = %d, want 1", foundCount)
	}
	if got := cache.Stats().DeletedEntryCount; got != 1 {
		t.Fatalf("DeletedEntryCount = %d, want 1", got)
	}
}

func TestCacheGetAndDeleteExpiredEntry(t *testing.T) {
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

	value, found := cache.GetAndDelete("expired")
	if found || value != 0 {
		t.Fatalf("GetAndDelete(expired) = (%d, %t), want (0, false)", value, found)
	}
	if cache.Exists("expired") {
		t.Fatal("expired entry remains resident")
	}

	after := cache.Stats()
	if after.ExpirationCount != before.ExpirationCount+1 {
		t.Fatalf("ExpirationCount = %d, want %d", after.ExpirationCount, before.ExpirationCount+1)
	}
	if after.DeletedEntryCount != before.DeletedEntryCount {
		t.Fatalf("expired entry changed DeletedEntryCount to %d", after.DeletedEntryCount)
	}
}

func TestCacheGetAndDeleteSupersedesInflightLoad(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(8), WithSegmentCount(1))

	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)

	go func() {
		_, _, err := cache.GetOrLoadWith(
			context.Background(),
			"key",
			func(context.Context, string) (int, bool, error) {
				close(started)
				<-release

				return 42, true, nil
			},
		)
		result <- err
	}()

	waitTestSignal(t, started)

	value, found := cache.GetAndDelete("key")
	if found || value != 0 {
		t.Fatalf("GetAndDelete(key) during load = (%d, %t), want (0, false)", value, found)
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
	if stats.DeletedEntryCount != 0 {
		t.Fatalf("DeletedEntryCount = %d, want 0 for missing resident key", stats.DeletedEntryCount)
	}
}

func TestZeroValueCacheGetAndDelete(t *testing.T) {
	var cache Cache[string, int]

	value, found := cache.GetAndDelete("key")
	if found || value != 0 {
		t.Fatalf("GetAndDelete() = (%d, %t), want (0, false)", value, found)
	}
}

func TestDeleteMultipleKeysAndDuplicates(t *testing.T) {
	tests := []struct {
		name    string
		options []Option
	}{
		{
			name:    "single_segment",
			options: []Option{WithMaxEntries(64)},
		},
		{
			name: "multiple_segments",
			options: []Option{
				WithMaxEntries(64),
				WithSegmentCount(4),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := mustNewCache[int](t, "users", test.options...)
			cache.Set("a", 1, NoExpiration)
			cache.Set("b", 2, NoExpiration)
			cache.Set("c", 3, NoExpiration)

			cache.Delete("a", "b", "a", "missing")

			if _, found := cache.Get("a"); found {
				t.Fatal("a remains cached")
			}
			if _, found := cache.Get("b"); found {
				t.Fatal("b remains cached")
			}
			if value, found := cache.Get("c"); value != 3 || !found {
				t.Fatalf("c = (%d, %t), want retained hit", value, found)
			}
			if got := cache.Stats().DeletedEntryCount; got != 2 {
				t.Fatalf("DeletedEntryCount = %d, want 2", got)
			}
		})
	}
}

func TestConcurrentMultiKeyDeletionLockOrderDoesNotDeadlock(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(16), WithSegmentCount(8))
	cache.Set("a", 1, NoExpiration)
	cache.Set("b", 2, NoExpiration)

	var group sync.WaitGroup
	group.Add(2)
	for _, keys := range [][]string{{"a", "b"}, {"b", "a"}} {
		go func() {
			defer group.Done()
			for range 1_000 {
				cache.Delete(keys...)
			}
		}()
	}

	waitTestGroup(t, &group)
}

func TestCacheClear(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(8), WithSegmentCount(2))
	cache.Set("a", 1, NoExpiration)
	cache.Set("b", 2, NoExpiration)

	before := cache.Stats()

	cache.Clear()

	if cache.Exists("a") || cache.Exists("b") {
		t.Fatal("entries remain cached after Clear")
	}

	after := cache.Stats()
	if after.EntryCount != 0 {
		t.Fatalf("EntryCount = %d, want 0", after.EntryCount)
	}
	if after.ClearedEntryCount != before.ClearedEntryCount+2 {
		t.Fatalf(
			"ClearedEntryCount = %d, want %d",
			after.ClearedEntryCount,
			before.ClearedEntryCount+2,
		)
	}
	if after.DeletedEntryCount != before.DeletedEntryCount {
		t.Fatalf("Clear changed DeletedEntryCount to %d", after.DeletedEntryCount)
	}

	cache.Clear()
	if got := cache.Stats().ClearedEntryCount; got != after.ClearedEntryCount {
		t.Fatalf("empty Clear changed ClearedEntryCount to %d", got)
	}
}

func TestCacheDeleteExpired(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
	cache := &Cache[string, int]{
		name:   "test",
		store:  store,
		states: make([]cacheState[string, int], 1),
		stats:  newStatsCollector(1),
		cleanupPolicy: cleanupPolicy{
			batchSize:   defaultCleanupBatchSize,
			entryBudget: defaultCleanupEntryBudget,
		},
		ttl: time.Minute,
	}

	store.setAt(0, "expired", 1, time.Nanosecond, -1, cache.stats.segment(0))
	store.setAt(0, "live", 2, time.Hour, int64(time.Hour), cache.stats.segment(0))

	if removed := cache.DeleteExpired(); removed != 1 {
		t.Fatalf("DeleteExpired() = %d, want 1", removed)
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
