package pacecache

import (
	"testing"
	"time"
)

func TestCacheExists(t *testing.T) {
	cache := mustNewCache[int](
		t,
		"users",
		WithMaxEntries(8),
		WithSegmentCount(1),
	)

	cache.Set("positive", 1, time.Minute)
	before := cache.Stats()

	if !cache.Exists("positive") {
		t.Fatal("Exists(positive) = false, want true")
	}
	if cache.Exists("missing") {
		t.Fatal("Exists(missing) = true, want false")
	}

	after := cache.Stats()
	if after.HitCount != before.HitCount || after.MissCount != before.MissCount {
		t.Fatalf("Exists changed lookup stats: before=%+v after=%+v", before, after)
	}
}

func TestCacheRefreshTTL(t *testing.T) {
	cache := mustNewCache[int](
		t,
		"users",
		WithMaxEntries(8),
		WithSegmentCount(1),
		WithTTL(time.Minute),
	)

	segment := &cache.store.segments[0]
	cache.Set("expiring", 1, DefaultExpiration)
	item := segment.entries["expiring"]
	selectedTTL := item.refreshTTL
	if selectedTTL != time.Minute {
		t.Fatalf("refreshTTL = %v, want 1m", selectedTTL)
	}

	segment.mu.Lock()
	forcedDeadline := cache.store.now() + int64(time.Millisecond)
	segment.expirations.update(item, forcedDeadline)
	segment.mu.Unlock()

	before := cache.Stats()
	if !cache.RefreshTTL("expiring") {
		t.Fatal("RefreshTTL(expiring) = false, want true")
	}
	if item.deadline <= forcedDeadline {
		t.Fatalf("deadline = %d, want > %d", item.deadline, forcedDeadline)
	}
	if item.refreshTTL != selectedTTL {
		t.Fatalf("RefreshTTL changed selected TTL: got %v, want %v", item.refreshTTL, selectedTTL)
	}

	cache.Set("forever", 2, NoExpiration)
	forever := segment.entries["forever"]
	if !cache.RefreshTTL("forever") {
		t.Fatal("RefreshTTL(forever) = false, want true")
	}
	if forever.deadline != 0 || forever.refreshTTL != 0 {
		t.Fatalf("NoExpiration entry changed: deadline=%d refreshTTL=%v", forever.deadline, forever.refreshTTL)
	}

	if cache.RefreshTTL("missing") {
		t.Fatal("RefreshTTL(missing) = true, want false")
	}

	after := cache.Stats()
	if after.HitCount != before.HitCount || after.MissCount != before.MissCount {
		t.Fatalf("RefreshTTL changed lookup stats: before=%+v after=%+v", before, after)
	}
}

func TestCacheExistsAndRefreshTTLRemoveExpiredEntries(t *testing.T) {
	tests := []struct {
		name string
		call func(*Cache[string, int], string) bool
	}{
		{name: "exists", call: func(cache *Cache[string, int], key string) bool { return cache.Exists(key) }},
		{name: "refresh_ttl", call: func(cache *Cache[string, int], key string) bool { return cache.RefreshTTL(key) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := mustNewCache[int](t, "users", WithMaxEntries(2), WithSegmentCount(1))
			stats := cache.stats.segment(0)
			deadline := cache.store.now()

			cache.store.setAt(
				0,
				"expired",
				1,
				time.Minute,
				deadline,
				stats,
			)

			before := cache.Stats()
			if test.call(cache, "expired") {
				t.Fatalf("%s(expired) = true, want false", test.name)
			}
			if _, ok := cache.store.segments[0].entries["expired"]; ok {
				t.Fatalf("%s left expired entry resident", test.name)
			}

			after := cache.Stats()
			if after.ExpirationCount != before.ExpirationCount+1 {
				t.Fatalf("ExpirationCount = %d, want %d", after.ExpirationCount, before.ExpirationCount+1)
			}
			if after.HitCount != before.HitCount || after.MissCount != before.MissCount {
				t.Fatalf("%s changed lookup stats: before=%+v after=%+v", test.name, before, after)
			}
		})
	}
}

func TestCacheExistsAndRefreshTTLDoNotUpdateLRU(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(2), WithSegmentCount(1))

	cache.Set("a", 1, time.Minute)
	cache.Set("b", 2, time.Minute)

	segment := &cache.store.segments[0]
	if segment.head.key != "b" || segment.tail.key != "a" {
		t.Fatalf("initial LRU = head:%q tail:%q, want b/a", segment.head.key, segment.tail.key)
	}

	if !cache.Exists("a") || !cache.RefreshTTL("a") {
		t.Fatal("a must remain live")
	}
	if segment.head.key != "b" || segment.tail.key != "a" {
		t.Fatalf("LRU after Exists/RefreshTTL = head:%q tail:%q, want b/a", segment.head.key, segment.tail.key)
	}

	cache.Set("c", 3, time.Minute)
	if _, ok := segment.entries["a"]; ok {
		t.Fatal("a remains resident; Exists/RefreshTTL updated recency")
	}
}

func TestCacheSetGetAndLRU(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(2), WithSegmentCount(1))

	if value, found := cache.Get("missing"); value != 0 || found {
		t.Fatalf("Get(missing) = (%d, %t), want (0, false)", value, found)
	}

	cache.Set("a", 1, DefaultExpiration)
	cache.Set("b", 2, NoExpiration)

	if value, found := cache.Get("a"); value != 1 || !found {
		t.Fatalf("Get(a) = (%d, %t), want (1, true)", value, found)
	}

	// a is MRU, so c evicts b.
	cache.Set("c", 3, time.Minute)

	if _, found := cache.Get("b"); found {
		t.Fatal("b remains resident after eviction")
	}
	if value, found := cache.Get("a"); value != 1 || !found {
		t.Fatalf("Get(a) = (%d, %t), want retained hit", value, found)
	}
	if value, found := cache.Get("c"); value != 3 || !found {
		t.Fatalf("Get(c) = (%d, %t), want retained hit", value, found)
	}
	if got := cache.Stats().EvictionCount; got != 1 {
		t.Fatalf("EvictionCount = %d, want 1", got)
	}
}

func TestCacheGetEntry(t *testing.T) {
	cache := mustNewCache[int](
		t,
		"users",
		WithMaxEntries(4),
		WithSegmentCount(1),
		WithTTL(time.Minute),
	)

	cache.Set("expiring", 42, DefaultExpiration)
	entry, found := cache.GetEntry("expiring")
	if !found || entry.Value() != 42 || entry.ExpiresAt().IsZero() {
		t.Fatalf("GetEntry(expiring) = (%+v, %t), want value=42 with deadline", entry, found)
	}

	cache.Set("forever", 7, NoExpiration)
	entry, found = cache.GetEntry("forever")
	if !found || entry.Value() != 7 || !entry.ExpiresAt().IsZero() {
		t.Fatalf("GetEntry(forever) = (%+v, %t), want value=7 without deadline", entry, found)
	}

	entry, found = cache.GetEntry("missing")
	if found || entry != (Entry[int]{}) {
		t.Fatalf("GetEntry(missing) = (%+v, %t), want zero/false", entry, found)
	}
}
