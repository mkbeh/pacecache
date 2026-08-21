package pacecache

import (
	"testing"
	"time"
)

func TestNewStorageDistributesCapacity(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](10, 3, time.Second)
	want := []int{4, 3, 3}

	if store.maxEntries != 10 {
		t.Fatalf("maxEntries = %d, want 10", store.maxEntries)
	}
	if len(store.segments) != 3 {
		t.Fatalf("segments = %d, want 3", len(store.segments))
	}
	for index, capacity := range want {
		if store.segments[index].maxEntries != capacity {
			t.Fatalf("segment %d capacity = %d, want %d", index, store.segments[index].maxEntries, capacity)
		}
		if store.segments[index].expirations.resolutionNanos != int64(time.Second) {
			t.Fatalf("segment %d expiration resolution = %d", index, store.segments[index].expirations.resolutionNanos)
		}
	}
}

func TestNewStorageAppliesInternalDefaults(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](0, 0, time.Second)
	if store.maxEntries != defaultMaxEntries {
		t.Fatalf("maxEntries = %d, want %d", store.maxEntries, defaultMaxEntries)
	}
	if len(store.segments) != defaultStorageSegmentCount {
		t.Fatalf("segments = %d, want %d", len(store.segments), defaultStorageSegmentCount)
	}
}

func TestNewStorageWithSegmentsAllowsZeroCapacityInternalSegments(t *testing.T) {
	store := newStorageWithSegments[string, int](1, 2)
	if store.segments[0].maxEntries != 1 || store.segments[1].maxEntries != 0 {
		t.Fatalf("segment capacities = (%d, %d), want (1, 0)", store.segments[0].maxEntries, store.segments[1].maxEntries)
	}

	store.segments[1].set("key", 1, 0, 0, nil)
	if len(store.segments[1].entries) != 0 {
		t.Fatal("zero-capacity internal segment stored an entry")
	}
}

func TestStorageSegmentIndex(t *testing.T) {
	one := newStorageWithSegments[string, int](10, 1)
	for _, key := range []string{"", "a", "key", "another-key"} {
		if got := one.segmentIndex(key); got != 0 {
			t.Fatalf("one-segment index for %q = %d, want 0", key, got)
		}
	}

	for _, segmentCount := range []int{3, 4, 32} {
		store := newStorageWithSegments[string, int](128, segmentCount)
		for index := range 1_000 {
			got := store.segmentIndex(string(rune(index)))
			if got < 0 || got >= segmentCount {
				t.Fatalf("segmentIndex = %d, want [0,%d)", got, segmentCount)
			}
		}
	}
}

func TestStorageTimeHelpers(t *testing.T) {
	store := newStorageWithSegments[string, int](1, 1)
	if store.now() < 0 {
		t.Fatal("now() returned negative elapsed time")
	}

	if got := store.expiresAt(0); !got.IsZero() {
		t.Fatalf("expiresAt(0) = %v, want zero time", got)
	}
	if got := store.expiresAt(123); !got.Equal(store.origin.Add(123 * time.Nanosecond)) {
		t.Fatalf("expiresAt(123) = %v, want %v", got, store.origin.Add(123*time.Nanosecond))
	}

	var nilStore *storage[string, int]
	if got := nilStore.expiresAt(123); !got.IsZero() {
		t.Fatalf("nil storage expiresAt = %v, want zero time", got)
	}
}

func TestStorageEnableExpirationAndSliding(t *testing.T) {
	store := newStorageWithSegments[string, int](4, 2)
	for index := range store.segments {
		if store.segments[index].expirations.enabled() || store.segments[index].slidingExpiration {
			t.Fatalf("segment %d feature unexpectedly enabled", index)
		}
	}

	store.enableExpirationIndex(0)
	for index := range store.segments {
		if store.segments[index].expirations.enabled() {
			t.Fatalf("segment %d expiration index enabled for zero resolution", index)
		}
	}

	store.enableExpirationIndex(time.Second)
	store.enableSlidingExpiration()
	for index := range store.segments {
		if !store.segments[index].expirations.enabled() || !store.segments[index].slidingExpiration {
			t.Fatalf("segment %d features not enabled", index)
		}
	}
}

func TestStorageCleanupExpiredYieldsBetweenBatches(t *testing.T) {
	const entries = defaultCleanupBatchSize + 10
	store := newStorageWithExpirationResolution[string, int](entries, 1, time.Nanosecond)
	stats := newStatsCollector(1)

	for index := range entries {
		key := string(rune(index + 1))
		store.setAt(0, key, index, time.Nanosecond, 1, stats.segment(0))
	}

	removed := store.cleanupExpired(
		2,
		cleanupPolicy{batchSize: defaultCleanupBatchSize, entryBudget: defaultCleanupEntryBudget},
		stats,
	)
	if removed != entries {
		t.Fatalf("cleanupExpired() = %d, want %d", removed, entries)
	}
}

func TestStorageCleanupExpiredDrainsAllDueEntries(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](32, 2, time.Nanosecond)
	stats := newStatsCollector(2)

	for index, key := range []string{"a", "b", "c", "d"} {
		segment := store.segmentIndex(key)
		store.setAt(segment, key, index, time.Nanosecond, 10, stats.segment(segment))
	}
	liveSegment := store.segmentIndex("live")
	store.setAt(liveSegment, "live", 9, time.Nanosecond, 1_000, stats.segment(liveSegment))

	removed := store.cleanupExpired(
		20,
		cleanupPolicy{batchSize: defaultCleanupBatchSize, entryBudget: defaultCleanupEntryBudget},
		stats,
	)
	if removed != 4 {
		t.Fatalf("cleanupExpired() = %d, want 4", removed)
	}
	if got := int64(len(store.segments[0].entries) + len(store.segments[1].entries)); got != 1 {
		t.Fatalf("resident entries = %d, want 1", got)
	}
}
