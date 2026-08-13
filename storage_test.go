package pacecache

import (
	"fmt"
	"testing"
	"time"
)

func TestStorageSegmentIndexIsStableAndInRange(t *testing.T) {
	for _, segmentCount := range []int{1, 3, 8, 32} {
		t.Run(fmt.Sprintf("segments=%d", segmentCount), func(t *testing.T) {
			storage := newStorage[int](128, segmentCount)

			for index := 0; index < 1000; index++ {
				key := fmt.Sprintf("key-%d", index)
				first := storage.segmentIndex(key)
				second := storage.segmentIndex(key)

				if first != second {
					t.Fatalf("segmentIndex(%q) changed: %d != %d", key, first, second)
				}
				if first < 0 || first >= segmentCount {
					t.Fatalf("segmentIndex(%q) = %d, want [0,%d)", key, first, segmentCount)
				}
			}
		})
	}
}

func TestStorageCapacityIsDistributedExactly(t *testing.T) {
	storage := newStorage[int](10, 3)

	got := []int{
		storage.segments[0].maxEntries,
		storage.segments[1].maxEntries,
		storage.segments[2].maxEntries,
	}
	want := []int{4, 3, 3}

	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("segment %d capacity = %d, want %d", index, got[index], want[index])
		}
	}
}

func TestStorageSegmentExactLRU(t *testing.T) {
	segment := storageSegment[int]{
		entries:    make(map[string]*entry[int]),
		maxEntries: 2,
	}
	stats := &statsShard{}
	now := time.Unix(1_000, 0)
	expiresAt := now.Add(time.Hour)

	segment.set("a", cachedValue[int]{value: 1, found: true}, expiresAt, stats)
	segment.set("b", cachedValue[int]{value: 2, found: true}, expiresAt, stats)

	// Touch a so b becomes the LRU victim.
	if _, ok := segment.lookup("a", now, stats); !ok {
		t.Fatal("lookup(a) = miss, want hit")
	}

	segment.set("c", cachedValue[int]{value: 3, found: true}, expiresAt, stats)

	if _, ok := segment.get("a", now, stats); !ok {
		t.Fatal("a was evicted, want resident")
	}
	if _, ok := segment.get("b", now, stats); ok {
		t.Fatal("b is resident, want evicted")
	}
	if value, ok := segment.get("c", now, stats); !ok || value.value != 3 {
		t.Fatalf("c = (%+v, %t), want value=3 hit", value, ok)
	}
	if stats.evictionCount != 1 {
		t.Fatalf("evictionCount = %d, want 1", stats.evictionCount)
	}
}

func TestStorageSegmentUpdateDoesNotEvict(t *testing.T) {
	segment := storageSegment[int]{
		entries:    make(map[string]*entry[int]),
		maxEntries: 1,
	}
	stats := &statsShard{}
	now := time.Unix(1_000, 0)

	segment.set("a", cachedValue[int]{value: 1, found: true}, now.Add(time.Minute), stats)
	segment.set("a", cachedValue[int]{value: 2, found: true}, now.Add(2*time.Minute), stats)

	got, ok := segment.get("a", now, stats)
	if !ok || got.value != 2 {
		t.Fatalf("get(a) = (%+v, %t), want value=2 hit", got, ok)
	}
	if stats.evictionCount != 0 {
		t.Fatalf("evictionCount = %d, want 0", stats.evictionCount)
	}
}

func TestStorageSegmentExpirationIsLazy(t *testing.T) {
	segment := storageSegment[int]{
		entries:    make(map[string]*entry[int]),
		maxEntries: 1,
	}
	stats := &statsShard{}
	now := time.Unix(1_000, 0)

	segment.set("a", cachedValue[int]{value: 1, found: true}, now.Add(time.Second), stats)

	if len(segment.entries) != 1 {
		t.Fatalf("resident entries = %d, want 1", len(segment.entries))
	}

	if _, ok := segment.lookup("a", now.Add(time.Second), stats); ok {
		t.Fatal("lookup(expired) = hit, want miss")
	}
	if len(segment.entries) != 0 {
		t.Fatalf("resident entries after lookup = %d, want 0", len(segment.entries))
	}
	if stats.expirationCount != 1 {
		t.Fatalf("expirationCount = %d, want 1", stats.expirationCount)
	}
	if stats.missCount != 1 {
		t.Fatalf("missCount = %d, want 1", stats.missCount)
	}
}

func TestStorageSegmentLookupStatistics(t *testing.T) {
	segment := storageSegment[int]{
		entries:    make(map[string]*entry[int]),
		maxEntries: 2,
	}
	stats := &statsShard{}
	now := time.Unix(1_000, 0)
	expiresAt := now.Add(time.Minute)

	segment.set("positive", cachedValue[int]{value: 42, found: true}, expiresAt, stats)
	segment.set("negative", cachedValue[int]{found: false}, expiresAt, stats)

	segment.lookup("positive", now, stats)
	segment.lookup("negative", now, stats)
	segment.lookup("missing", now, stats)

	if stats.hitCount != 1 {
		t.Fatalf("hitCount = %d, want 1", stats.hitCount)
	}
	if stats.negativeHitCount != 1 {
		t.Fatalf("negativeHitCount = %d, want 1", stats.negativeHitCount)
	}
	if stats.missCount != 1 {
		t.Fatalf("missCount = %d, want 1", stats.missCount)
	}
}

func TestStorageDeleteAll(t *testing.T) {
	storage := newStorage[int](16, 4)
	now := time.Unix(1_000, 0)

	for _, key := range []string{"a", "b", "c", "d"} {
		index := storage.segmentIndex(key)
		storage.setAt(index, key, cachedValue[int]{value: 1, found: true}, now.Add(time.Hour), nil)
	}

	deleted := storage.deleteAll()
	if deleted != 4 {
		t.Fatalf("deleteAll() = %d, want 4", deleted)
	}

	for index := range storage.segments {
		segment := &storage.segments[index]
		if len(segment.entries) != 0 || segment.head != nil || segment.tail != nil {
			t.Fatalf("segment %d was not fully cleared", index)
		}
	}
}
