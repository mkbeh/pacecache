package pacecache

import (
	"testing"
	"time"
)

func TestStorageSetLookupUpdateAndLRU(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 1, time.Nanosecond)
	stats := &segmentStats{}

	store.setAt(0, "a", 1, 100*time.Nanosecond, 1_000, stats)
	store.setAt(0, "b", 2, 100*time.Nanosecond, 1_000, stats)

	segment := &store.segments[0]
	if segment.head.key != "b" || segment.tail.key != "a" {
		t.Fatalf("LRU = head:%q tail:%q, want b/a", segment.head.key, segment.tail.key)
	}

	value, ok := store.lookupAt(0, "a", 100, stats)
	if !ok || value != 1 {
		t.Fatalf("lookupAt(a) = (%d, %t), want (1, true)", value, ok)
	}
	if segment.head.key != "a" || segment.tail.key != "b" {
		t.Fatalf("LRU after hit = head:%q tail:%q, want a/b", segment.head.key, segment.tail.key)
	}

	victim := segment.tail
	store.setAt(0, "c", 3, 100*time.Nanosecond, 1_000, stats)
	if stats.evictionCount != 1 {
		t.Fatalf("evictionCount = %d, want 1", stats.evictionCount)
	}
	if segment.entries["c"] != victim {
		t.Fatal("LRU victim was not reused")
	}
	if _, ok := segment.entries["b"]; ok {
		t.Fatal("evicted key b remains resident")
	}

	store.setAt(0, "a", 10, 0, 0, stats)
	if segment.entries["a"].value != 10 || segment.entries["a"].deadline != 0 || segment.entries["a"].refreshTTL != 0 {
		t.Fatalf("updated entry = %+v, want value=10 no expiration", segment.entries["a"])
	}
}

func TestStorageLookupStatistics(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
	stats := &segmentStats{}

	if _, ok := store.lookupAt(0, "missing", 1, stats); ok {
		t.Fatal("missing lookup unexpectedly hit")
	}
	store.setAt(0, "key", 1, 0, 0, stats)
	if _, ok := store.lookupAt(0, "key", 1, stats); !ok {
		t.Fatal("live lookup missed")
	}

	if stats.missCount != 1 || stats.hitCount != 1 {
		t.Fatalf("lookup stats = miss:%d hit:%d, want 1/1", stats.missCount, stats.hitCount)
	}
}

func TestStorageLookupEntryCarriesDeadlineAndStatistics(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 1, time.Nanosecond)
	stats := &segmentStats{}
	store.setAt(0, "key", 7, 100*time.Nanosecond, 200, stats)

	cached, ok := store.lookupEntryAt(0, "key", 100, stats)
	if !ok || cached.value != 7 || cached.deadline != 200 {
		t.Fatalf("lookupEntryAt() = (%+v, %t), want value=7 deadline=200", cached, ok)
	}
	if stats.hitCount != 1 || stats.missCount != 0 {
		t.Fatalf("lookup stats = hit:%d miss:%d, want 1/0", stats.hitCount, stats.missCount)
	}
}

func TestStorageGetEntryAtDoesNotRecordLookupHitOrMiss(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 1, time.Nanosecond)
	stats := &segmentStats{}
	store.setAt(0, "key", 7, 0, 0, stats)

	if cached, ok := store.getEntryAt(0, "key", 1, stats); !ok || cached.value != 7 {
		t.Fatalf("getEntryAt(key) = (%+v, %t), want live value", cached, ok)
	}
	if _, ok := store.getEntryAt(0, "missing", 1, stats); ok {
		t.Fatal("getEntryAt(missing) unexpectedly hit")
	}
	if stats.hitCount != 0 || stats.missCount != 0 {
		t.Fatalf("getEntryAt changed lookup stats: hit=%d miss=%d", stats.hitCount, stats.missCount)
	}
}

func TestStorageLazyExpirationRemovesEntry(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 1, time.Nanosecond)
	stats := &segmentStats{}
	store.setAt(0, "key", 1, 50*time.Nanosecond, 100, stats)

	if _, ok := store.lookupAt(0, "key", 100, stats); ok {
		t.Fatal("expired entry unexpectedly returned")
	}
	if len(store.segments[0].entries) != 0 {
		t.Fatal("expired entry remains resident")
	}
	if stats.expirationCount != 1 || stats.missCount != 1 {
		t.Fatalf("stats = expiration:%d miss:%d, want 1/1", stats.expirationCount, stats.missCount)
	}
}

func TestStorageExistsSemantics(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
	stats := &segmentStats{}

	store.setAt(0, "live", 1, 0, 0, stats)
	if !store.existsAt(0, "live", 100, stats) {
		t.Fatal("existsAt(live) = false, want true")
	}
	if store.existsAt(0, "missing", 100, stats) {
		t.Fatal("existsAt(missing) = true, want false")
	}
	if stats.hitCount != 0 || stats.missCount != 0 {
		t.Fatalf("existsAt changed lookup stats: hit=%d miss=%d", stats.hitCount, stats.missCount)
	}

	store.setAt(0, "expired", 2, 50*time.Nanosecond, 100, stats)
	if store.existsAt(0, "expired", 100, stats) {
		t.Fatal("existsAt(expired) = true, want false")
	}
	if _, ok := store.segments[0].entries["expired"]; ok {
		t.Fatal("existsAt left expired entry resident")
	}
	if stats.expirationCount != 1 {
		t.Fatalf("expirationCount = %d, want 1", stats.expirationCount)
	}
}

func TestStorageRefreshTTLOnlyExtendsDeadline(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 1, time.Nanosecond)
	store.setAt(0, "key", 1, 100*time.Nanosecond, 200, nil)

	if !store.refreshTTLAt(0, "key", 150, nil) {
		t.Fatal("refreshTTLAt(key) = false, want true")
	}
	item := store.segments[0].entries["key"]
	if item.deadline != 250 {
		t.Fatalf("deadline after refresh = %d, want 250", item.deadline)
	}

	if !store.refreshTTLAt(0, "key", 120, nil) {
		t.Fatal("refreshTTLAt(key) with stale timestamp = false")
	}
	if item.deadline != 250 {
		t.Fatalf("stale refresh shortened deadline to %d", item.deadline)
	}

	store.setAt(0, "key", 2, 50*time.Nanosecond, 1_050, nil)
	if !store.refreshTTLAt(0, "key", 100, nil) {
		t.Fatal("refreshTTLAt(key) after Set = false")
	}
	if item.deadline != 1_050 {
		t.Fatalf("stale refresh shortened Set deadline to %d", item.deadline)
	}
}

func TestStorageRefreshTTLSemantics(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
	stats := &segmentStats{}

	store.setAt(0, "forever", 1, 100*time.Nanosecond, 0, stats)
	forever := store.segments[0].entries["forever"]
	if !store.refreshTTLAt(0, "forever", 100, stats) {
		t.Fatal("refreshTTLAt(forever) = false, want true")
	}
	if forever.refreshTTL != 0 || forever.deadline != 0 {
		t.Fatalf("NoExpiration entry = refreshTTL:%v deadline:%d, want 0/0", forever.refreshTTL, forever.deadline)
	}

	if store.refreshTTLAt(0, "missing", 100, stats) {
		t.Fatal("refreshTTLAt(missing) = true")
	}
	if stats.hitCount != 0 || stats.missCount != 0 {
		t.Fatalf("refreshTTLAt changed lookup stats: hit=%d miss=%d", stats.hitCount, stats.missCount)
	}

	store.setAt(0, "expired", 2, 50*time.Nanosecond, 100, stats)
	if store.refreshTTLAt(0, "expired", 100, stats) {
		t.Fatal("refreshTTLAt(expired) = true")
	}
	if _, ok := store.segments[0].entries["expired"]; ok {
		t.Fatal("refreshTTLAt left expired entry resident")
	}
	if stats.expirationCount != 1 {
		t.Fatalf("expirationCount = %d, want 1", stats.expirationCount)
	}
}

func TestStorageSlidingExpirationOnlyExtendsDeadline(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 1, time.Nanosecond)
	store.enableSlidingExpiration()
	store.setAt(0, "key", 1, 100*time.Nanosecond, 200, nil)

	if _, ok := store.lookupAt(0, "key", 150, nil); !ok {
		t.Fatal("live entry missed")
	}
	item := store.segments[0].entries["key"]
	if item.deadline != 250 {
		t.Fatalf("deadline after newer read = %d, want 250", item.deadline)
	}

	if _, ok := store.lookupAt(0, "key", 120, nil); !ok {
		t.Fatal("live entry missed on stale timestamp")
	}
	if item.deadline != 250 {
		t.Fatalf("stale read shortened deadline to %d", item.deadline)
	}

	store.setAt(0, "key", 2, 50*time.Nanosecond, 1_050, nil)
	if _, ok := store.lookupAt(0, "key", 100, nil); !ok {
		t.Fatal("entry missed after Set")
	}
	if item.deadline != 1_050 {
		t.Fatalf("stale read shortened Set deadline to %d", item.deadline)
	}
}

func TestStorageSlidingExpirationSkipsNoExpiration(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 1, time.Nanosecond)
	store.enableSlidingExpiration()
	store.setAt(0, "forever", 1, 100*time.Nanosecond, 0, nil)

	item := store.segments[0].entries["forever"]
	if item.refreshTTL != 0 || item.deadline != 0 {
		t.Fatalf("NoExpiration entry = refreshTTL:%v deadline:%d, want 0/0", item.refreshTTL, item.deadline)
	}
	if _, ok := store.lookupAt(0, "forever", 1_000, nil); !ok {
		t.Fatal("NoExpiration entry missed")
	}
	if item.deadline != 0 {
		t.Fatalf("sliding expiration changed NoExpiration deadline to %d", item.deadline)
	}
}

func TestStorageDeleteAndDeleteAll(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
	store.setAt(0, "a", 1, 100*time.Nanosecond, 100, nil)
	store.setAt(0, "b", 2, 200*time.Nanosecond, 200, nil)

	if store.deleteAt(0, "missing") {
		t.Fatal("deleteAt(missing) = true")
	}
	if !store.deleteAt(0, "a") {
		t.Fatal("deleteAt(a) = false")
	}
	if _, ok := store.segments[0].entries["a"]; ok {
		t.Fatal("deleted entry a remains resident")
	}

	if deleted := store.deleteAll(); deleted != 1 {
		t.Fatalf("deleteAll() = %d, want 1", deleted)
	}
	segment := &store.segments[0]
	if len(segment.entries) != 0 || segment.head != nil || segment.tail != nil || len(segment.expirations.bucketHeap) != 0 {
		t.Fatalf("segment not reset: entries=%d head=%v tail=%v queue=%d", len(segment.entries), segment.head, segment.tail, len(segment.expirations.bucketHeap))
	}
}

func TestStorageSegmentCleanupExpiredHonorsLimitAndPending(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 1, 10*time.Nanosecond)
	stats := &segmentStats{}
	store.setAt(0, "a", 1, time.Nanosecond, 5, stats)
	store.setAt(0, "b", 2, time.Nanosecond, 15, stats)
	store.setAt(0, "live", 3, time.Nanosecond, 100, stats)

	removed, pending := store.cleanupExpiredAt(0, 25, 1, stats)
	if removed != 1 || !pending {
		t.Fatalf("first cleanup = (%d, %t), want (1, true)", removed, pending)
	}

	removed, pending = store.cleanupExpiredAt(0, 25, 1, stats)
	if removed != 1 || pending {
		t.Fatalf("second cleanup = (%d, %t), want (1, false)", removed, pending)
	}
	if stats.expirationCount != 2 {
		t.Fatalf("expirationCount = %d, want 2", stats.expirationCount)
	}
	if len(store.segments[0].entries) != 1 || store.segments[0].entries["live"] == nil {
		t.Fatal("cleanup removed live entry or left expired entries")
	}
}

func TestStorageSegmentCleanupExpiredNoopCases(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](1, 1, time.Nanosecond)
	store.setAt(0, "key", 1, time.Nanosecond, 1, nil)

	if removed, pending := store.cleanupExpiredAt(0, 10, 0, nil); removed != 0 || pending {
		t.Fatalf("cleanup with zero limit = (%d, %t), want (0, false)", removed, pending)
	}

	disabled := newStorageWithSegments[string, int](1, 1)
	if removed, pending := disabled.cleanupExpiredAt(0, 10, 1, nil); removed != 0 || pending {
		t.Fatalf("cleanup with disabled index = (%d, %t), want (0, false)", removed, pending)
	}
}
