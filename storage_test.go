package pacecache

import (
	"fmt"
	"testing"
	"time"
)

func TestNewStorage(t *testing.T) {
	t.Run("capacity distribution", func(t *testing.T) {
		store := newStorageWithSegments[string, int](10, 3)
		want := []int{4, 3, 3}

		if store.maxEntries != 10 {
			t.Fatalf("maxEntries = %d, want 10", store.maxEntries)
		}
		if len(store.segments) != len(want) {
			t.Fatalf("segments = %d, want %d", len(store.segments), len(want))
		}

		var total int
		for index := range store.segments {
			segment := &store.segments[index]
			if segment.maxEntries != want[index] {
				t.Fatalf("segment %d capacity = %d, want %d", index, segment.maxEntries, want[index])
			}
			total += segment.maxEntries
		}
		if total != store.maxEntries {
			t.Fatalf("segment capacity sum = %d, want %d", total, store.maxEntries)
		}
		if store.mask != 0 {
			t.Fatalf("mask = %d, want 0 for non-power-of-two segment count", store.mask)
		}
	})

	t.Run("power of two mask", func(t *testing.T) {
		store := newStorageWithSegments[string, int](8, 4)
		if store.mask != 3 {
			t.Fatalf("mask = %d, want 3", store.mask)
		}
	})

	t.Run("defaults and features", func(t *testing.T) {
		store := newStorage[string, int](0, 0, true)

		if store.maxEntries != defaultMaxEntries {
			t.Fatalf("maxEntries = %d, want %d", store.maxEntries, defaultMaxEntries)
		}
		if len(store.segments) != defaultStorageSegmentCount {
			t.Fatalf("segments = %d, want %d", len(store.segments), defaultStorageSegmentCount)
		}
		for index := range store.segments {
			segment := &store.segments[index]
			if !segment.expirations.enabled() {
				t.Fatalf("segment %d expiration index is disabled", index)
			}
			if !segment.slidingExpiration {
				t.Fatalf("segment %d sliding expiration is disabled", index)
			}
		}
	})

	t.Run("zero-capacity internal segment", func(t *testing.T) {
		store := newStorageWithSegments[string, int](2, 4)
		stats := newStatsCollector(4)

		if store.segments[2].maxEntries != 0 || store.segments[3].maxEntries != 0 {
			t.Fatal("expected trailing zero-capacity segments")
		}

		store.setAt(3, "key", cachedValue[int]{value: 1, found: true}, 0, stats.segment(3))
		if len(store.segments[3].entries) != 0 {
			t.Fatal("zero-capacity segment stored an entry")
		}
	})
}

func TestStorageSegmentIndex(t *testing.T) {
	one := newStorageWithSegments[string, int](1, 1)
	if got := one.segmentIndex("key"); got != 0 {
		t.Fatalf("single-segment index = %d, want 0", got)
	}

	strings := newStorageWithSegments[string, int](64, 8)
	first := strings.segmentIndex("stable")
	for range 100 {
		if got := strings.segmentIndex("stable"); got != first {
			t.Fatalf("segmentIndex is unstable: got %d, want %d", got, first)
		}
	}
	if first < 0 || first >= len(strings.segments) {
		t.Fatalf("segment index = %d, out of range", first)
	}

	integers := newStorageWithSegments[int64, int](64, 8)
	if got := integers.segmentIndex(42); got < 0 || got >= len(integers.segments) {
		t.Fatalf("int64 segment index = %d, out of range", got)
	}

	type compositeKey struct {
		Tenant string
		ID     int
	}
	composite := newStorageWithSegments[compositeKey, int](64, 8)
	key := compositeKey{Tenant: "acme", ID: 42}
	if got := composite.segmentIndex(key); got < 0 || got >= len(composite.segments) {
		t.Fatalf("composite segment index = %d, out of range", got)
	}
}

func TestStorageTimeConversions(t *testing.T) {
	store := newStorageWithSegments[string, int](1, 1)
	store.origin = time.Date(2026, time.August, 18, 0, 0, 0, 0, time.UTC)

	if got := store.deadlineAt(time.Time{}); got != 0 {
		t.Fatalf("deadlineAt(zero) = %d, want 0", got)
	}
	if got := store.deadlineAt(store.origin.Add(-time.Nanosecond)); got != -1 {
		t.Fatalf("deadlineAt(before origin) = %d, want -1", got)
	}
	if got := store.deadlineAt(store.origin.Add(25 * time.Nanosecond)); got != 25 {
		t.Fatalf("deadlineAt(after origin) = %d, want 25", got)
	}
	if got := store.expiresAt(0); !got.IsZero() {
		t.Fatalf("expiresAt(0) = %v, want zero time", got)
	}
	if got := store.expiresAt(25); !got.Equal(store.origin.Add(25 * time.Nanosecond)) {
		t.Fatalf("expiresAt(25) = %v", got)
	}

	var nilStore *storage[string, int]
	if got := nilStore.deadlineAt(time.Now()); got != 0 {
		t.Fatalf("nil deadlineAt() = %d, want 0", got)
	}
	if got := nilStore.expiresAt(25); !got.IsZero() {
		t.Fatalf("nil expiresAt() = %v, want zero time", got)
	}
}

func TestStorageSetUpdateLRUAndEviction(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 1, time.Nanosecond)
	stats := newStatsCollector(1)
	segment := &store.segments[0]

	store.setAt(0, "first", cachedValue[int]{value: 1, found: true}, 10, stats.segment(0))
	store.setAt(0, "second", cachedValue[int]{value: 2, found: true}, 20, stats.segment(0))

	if segment.head.key != "second" || segment.tail.key != "first" {
		t.Fatalf("LRU order = head %q tail %q", segment.head.key, segment.tail.key)
	}

	if value, ok := store.getAt(0, "first", 0, stats.segment(0)); !ok || value.value != 1 {
		t.Fatalf("getAt(first) = (%+v, %t)", value, ok)
	}
	if segment.head.key != "first" || segment.tail.key != "second" {
		t.Fatalf("LRU order after get = head %q tail %q", segment.head.key, segment.tail.key)
	}

	victim := segment.entries["second"]
	store.setAt(0, "third", cachedValue[int]{value: 3, found: true}, 30, stats.segment(0))

	if _, ok := segment.entries["second"]; ok {
		t.Fatal("LRU victim remains resident")
	}
	if segment.entries["third"] != victim {
		t.Fatal("eviction did not reuse the LRU entry")
	}
	if got := stats.segment(0).evictionCount; got != 1 {
		t.Fatalf("evictionCount = %d, want 1", got)
	}
	if segment.head.key != "third" || segment.tail.key != "first" {
		t.Fatalf("LRU order after eviction = head %q tail %q", segment.head.key, segment.tail.key)
	}

	store.setAt(0, "first", cachedValue[int]{value: 10, found: true}, 40, stats.segment(0))
	if got := segment.entries["first"].value; got != 10 {
		t.Fatalf("updated value = %d, want 10", got)
	}
	if got := segment.entries["first"].deadline; got != 40 {
		t.Fatalf("updated deadline = %d, want 40", got)
	}
	if got := stats.segment(0).evictionCount; got != 1 {
		t.Fatalf("evictionCount after update = %d, want 1", got)
	}
	if segment.head.key != "first" {
		t.Fatalf("updated entry is not MRU: head = %q", segment.head.key)
	}
}

func TestStorageSetNormalizesSlidingTTL(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](3, 1, time.Nanosecond)
	segment := &store.segments[0]

	segment.set("positive", cachedValue[int]{value: 1, found: true, refreshTTL: 10}, 100, nil)
	segment.set("negative", cachedValue[int]{found: false, refreshTTL: 10}, 100, nil)
	segment.set("forever", cachedValue[int]{value: 2, found: true, refreshTTL: 10}, 0, nil)

	if got := segment.entries["positive"].refreshTTL; got != 10 {
		t.Fatalf("positive refreshTTL = %v, want 10", got)
	}
	if got := segment.entries["negative"].refreshTTL; got != 0 {
		t.Fatalf("negative refreshTTL = %v, want 0", got)
	}
	if got := segment.entries["forever"].refreshTTL; got != 0 {
		t.Fatalf("NoExpiration refreshTTL = %v, want 0", got)
	}
}

func TestStorageLookupAndInternalGetStatistics(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
	stats := newStatsCollector(1)
	shard := stats.segment(0)

	store.setAt(0, "positive", cachedValue[int]{value: 1, found: true}, 100, shard)
	store.setAt(0, "negative", cachedValue[int]{found: false}, 100, shard)

	if _, ok := store.lookupAt(0, "positive", 10, shard); !ok {
		t.Fatal("positive lookup missed")
	}
	if _, ok := store.lookupEntryAt(0, "negative", 10, shard); !ok {
		t.Fatal("negative lookup missed")
	}
	if _, ok := store.lookupAt(0, "missing", 10, shard); ok {
		t.Fatal("missing lookup hit")
	}

	if _, ok := store.getAt(0, "positive", 10, shard); !ok {
		t.Fatal("internal get missed")
	}
	if _, ok := store.getEntryAt(0, "negative", 10, shard); !ok {
		t.Fatal("internal entry get missed")
	}

	if shard.hitCount != 1 || shard.negativeHitCount != 1 || shard.missCount != 1 {
		t.Fatalf("lookup stats = (hit=%d negative=%d miss=%d), want (1,1,1)", shard.hitCount, shard.negativeHitCount, shard.missCount)
	}

	if _, ok := store.lookupAt(0, "positive", 100, shard); ok {
		t.Fatal("expired positive entry was returned")
	}
	if _, ok := store.getEntryAt(0, "negative", 100, shard); ok {
		t.Fatal("expired negative entry was returned")
	}

	if shard.expirationCount != 2 {
		t.Fatalf("expirationCount = %d, want 2", shard.expirationCount)
	}
	if shard.missCount != 2 {
		t.Fatalf("missCount = %d, want 2", shard.missCount)
	}
}

func TestStorageExists(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
	stats := newStatsCollector(1)
	segment := &store.segments[0]
	shard := stats.segment(0)

	segment.set("positive", cachedValue[int]{value: 1, found: true}, 100, shard)
	segment.set("negative", cachedValue[int]{found: false}, 100, shard)
	segment.set("expired", cachedValue[int]{value: 2, found: true}, 5, shard)

	head := segment.head
	tail := segment.tail

	if !segment.exists("positive", 10, shard) {
		t.Fatal("Exists(positive) = false")
	}
	if segment.exists("negative", 10, shard) {
		t.Fatal("Exists(negative) = true")
	}
	if segment.exists("missing", 10, shard) {
		t.Fatal("Exists(missing) = true")
	}

	if segment.head != head || segment.tail != tail {
		t.Fatal("Exists changed LRU order for live entries")
	}
	if shard.hitCount != 0 || shard.negativeHitCount != 0 || shard.missCount != 0 {
		t.Fatal("Exists changed lookup statistics")
	}

	if segment.exists("expired", 10, shard) {
		t.Fatal("Exists(expired) = true")
	}
	if shard.expirationCount != 1 {
		t.Fatalf("expirationCount = %d, want 1", shard.expirationCount)
	}
}

func TestStorageRefreshTTL(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](5, 1, time.Nanosecond)
	stats := newStatsCollector(1)
	segment := &store.segments[0]
	shard := stats.segment(0)

	segment.set("positive", cachedValue[int]{value: 1, found: true, refreshTTL: 100}, 1000, shard)
	segment.set("negative", cachedValue[int]{found: false, refreshTTL: 100}, 1000, shard)
	segment.set("forever", cachedValue[int]{value: 2, found: true, refreshTTL: 100}, 0, shard)
	segment.set("expired", cachedValue[int]{value: 3, found: true, refreshTTL: 100}, 50, shard)

	head := segment.head
	tail := segment.tail

	if !segment.refreshTTL("positive", 100, shard) {
		t.Fatal("RefreshTTL(positive) = false")
	}
	if got := segment.entries["positive"].deadline; got != 1000 {
		t.Fatalf("stale refresh shortened deadline to %d", got)
	}

	if !segment.refreshTTL("positive", 950, shard) {
		t.Fatal("RefreshTTL(positive) = false")
	}
	if got := segment.entries["positive"].deadline; got != 1050 {
		t.Fatalf("extended deadline = %d, want 1050", got)
	}

	if segment.refreshTTL("negative", 100, shard) {
		t.Fatal("RefreshTTL(negative) = true")
	}
	if !segment.refreshTTL("forever", 100, shard) {
		t.Fatal("RefreshTTL(NoExpiration) = false")
	}
	if got := segment.entries["forever"].deadline; got != 0 {
		t.Fatalf("NoExpiration deadline = %d, want 0", got)
	}
	if segment.refreshTTL("missing", 100, shard) {
		t.Fatal("RefreshTTL(missing) = true")
	}

	if segment.head != head || segment.tail != tail {
		t.Fatal("RefreshTTL changed LRU order for live entries")
	}
	if shard.hitCount != 0 || shard.negativeHitCount != 0 || shard.missCount != 0 {
		t.Fatal("RefreshTTL changed lookup statistics")
	}

	if segment.refreshTTL("expired", 100, shard) {
		t.Fatal("RefreshTTL(expired) = true")
	}
	if shard.expirationCount != 1 {
		t.Fatalf("expirationCount = %d, want 1", shard.expirationCount)
	}
}

func TestStorageSlidingExpiration(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](3, 1, time.Nanosecond)
	store.enableSlidingExpiration()
	segment := &store.segments[0]

	segment.set("positive", cachedValue[int]{value: 1, found: true, refreshTTL: 100}, 1000, nil)
	segment.set("negative", cachedValue[int]{found: false, refreshTTL: 100}, 1000, nil)
	segment.set("forever", cachedValue[int]{value: 2, found: true, refreshTTL: 100}, 0, nil)

	entry, ok := segment.getEntry("positive", 950, nil)
	if !ok {
		t.Fatal("sliding positive entry missed")
	}
	if entry.deadline != 1050 {
		t.Fatalf("refreshed deadline = %d, want 1050", entry.deadline)
	}

	if _, ok := segment.get("positive", 100, nil); !ok {
		t.Fatal("positive entry missed on stale timestamp")
	}
	if got := segment.entries["positive"].deadline; got != 1050 {
		t.Fatalf("stale read shortened deadline to %d", got)
	}

	if _, ok := segment.getEntry("negative", 950, nil); !ok {
		t.Fatal("negative entry missed")
	}
	if got := segment.entries["negative"].deadline; got != 1000 {
		t.Fatalf("negative deadline = %d, want 1000", got)
	}

	if _, ok := segment.getEntry("forever", 950, nil); !ok {
		t.Fatal("NoExpiration entry missed")
	}
	if got := segment.entries["forever"].deadline; got != 0 {
		t.Fatalf("NoExpiration deadline = %d, want 0", got)
	}
}

func TestStorageDeleteAndDeleteAll(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
	segment := &store.segments[0]

	for index, key := range []string{"first", "second", "third"} {
		segment.set(key, cachedValue[int]{value: index, found: true}, int64(index+1), nil)
	}

	if segment.delete("missing") {
		t.Fatal("delete(missing) = true")
	}
	if !segment.delete("second") {
		t.Fatal("delete(second) = false")
	}
	if _, ok := segment.entries["second"]; ok {
		t.Fatal("deleted entry remains resident")
	}
	if got := segment.expirations.entryCount(); got != 2 {
		t.Fatalf("expiration entries = %d, want 2", got)
	}

	if got := store.deleteAll(); got != 2 {
		t.Fatalf("deleteAll() = %d, want 2", got)
	}
	if len(segment.entries) != 0 || segment.head != nil || segment.tail != nil {
		t.Fatal("deleteAll did not reset storage and LRU")
	}
	if len(segment.expirations.bucketHeap) != 0 || len(segment.expirations.buckets) != 0 {
		t.Fatal("deleteAll did not reset expiration index")
	}
	if got := store.deleteAll(); got != 0 {
		t.Fatalf("second deleteAll() = %d, want 0", got)
	}
}

func TestStorageCleanupExpired(t *testing.T) {
	t.Run("segment limit and pending", func(t *testing.T) {
		store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
		stats := newStatsCollector(1)
		segment := &store.segments[0]

		segment.set("first", cachedValue[int]{value: 1, found: true}, 1, stats.segment(0))
		segment.set("second", cachedValue[int]{value: 2, found: true}, 2, stats.segment(0))
		segment.set("live", cachedValue[int]{value: 3, found: true}, 100, stats.segment(0))

		removed, pending := segment.cleanupExpired(10, 1, stats.segment(0))
		if removed != 1 || !pending {
			t.Fatalf("first cleanup = (%d, %t), want (1, true)", removed, pending)
		}

		removed, pending = segment.cleanupExpired(10, 10, stats.segment(0))
		if removed != 1 || pending {
			t.Fatalf("second cleanup = (%d, %t), want (1, false)", removed, pending)
		}
		if len(segment.entries) != 1 || segment.entries["live"] == nil {
			t.Fatal("cleanup removed a live entry or retained expired entries")
		}
		if got := stats.segment(0).expirationCount; got != 2 {
			t.Fatalf("expirationCount = %d, want 2", got)
		}
	})

	t.Run("storage drains all segments", func(t *testing.T) {
		store := newStorageWithExpirationResolution[string, int](6, 2, time.Nanosecond)
		stats := newStatsCollector(2)

		store.setAt(0, "first", cachedValue[int]{value: 1, found: true}, 1, stats.segment(0))
		store.setAt(0, "second", cachedValue[int]{value: 2, found: true}, 2, stats.segment(0))
		store.setAt(1, "third", cachedValue[int]{value: 3, found: true}, 3, stats.segment(1))
		store.setAt(1, "live", cachedValue[int]{value: 4, found: true}, 100, stats.segment(1))

		removed := store.cleanupExpired(
			10,
			cleanupPolicy{batchSize: 1, entryBudget: 1},
			stats,
		)
		if removed != 3 {
			t.Fatalf("cleanupExpired() = %d, want 3", removed)
		}
		if len(store.segments[0].entries) != 0 || len(store.segments[1].entries) != 1 {
			t.Fatal("cleanup did not drain all due entries")
		}
		if got := stats.segment(0).expirationCount + stats.segment(1).expirationCount; got != 3 {
			t.Fatalf("expirationCount = %d, want 3", got)
		}
	})

	t.Run("noop cases", func(t *testing.T) {
		store := newStorageWithSegments[string, int](1, 1)
		segment := &store.segments[0]
		if removed, pending := segment.cleanupExpired(10, 0, nil); removed != 0 || pending {
			t.Fatalf("zero-limit cleanup = (%d, %t)", removed, pending)
		}
		if removed, pending := segment.cleanupExpired(10, 1, nil); removed != 0 || pending {
			t.Fatalf("disabled-index cleanup = (%d, %t)", removed, pending)
		}
	})
}

func TestStorageCleanupExpiredPanicsOnIndexMismatch(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](1, 1, time.Nanosecond)
	segment := &store.segments[0]
	segment.set("key", cachedValue[int]{value: 1, found: true}, 1, nil)

	segment.entries["key"] = &entry[string, int]{key: "key"}

	defer func() {
		value := recover()
		if value == nil {
			t.Fatal("panic = nil")
		}

		const want = "pacecache: expiration index is inconsistent with storage"
		if got := fmt.Sprint(value); got != want {
			t.Fatalf("panic = %q, want %q", got, want)
		}
	}()

	segment.cleanupExpired(10, 1, nil)
}
