package pacecache

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestStatsSnapshotAggregatesSegments(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](4, 2, time.Nanosecond)
	collector := newStatsCollector(2)
	cache := &Cache[string, int]{
		name:   "users",
		store:  store,
		states: make([]cacheState[string, int], 2),
		stats:  collector,
		ttl:    time.Minute,
	}

	for index := range store.segments {
		segment := &store.segments[index]
		key := string(rune('a' + index))

		segment.mu.Lock()
		segment.entries[key] = &entry[string, int]{key: key}
		counters := collector.segment(index)
		counters.hitCount = int64(index + 1)
		counters.missCount = int64(index + 3)
		counters.evictionCount = int64(index + 4)
		counters.expirationCount = int64(index + 5)
		segment.mu.Unlock()

		counters.loadFoundCount.Store(int64(index + 6))
		counters.loadNotFoundCount.Store(int64(index + 7))
		counters.loadErrorCount.Store(int64(index + 8))
		counters.loadSupersededCount.Store(int64(index + 9))
		counters.loadDurationNanos.Store(int64((index + 10) * 100))
		counters.sharedCount.Store(int64(index + 11))
		counters.deletedEntryCount.Store(int64(index + 12))
	}

	collector.clearedEntryCount.Store(13)
	collector.cleanupCount.Store(14)
	collector.cleanupWorkerRunCount.Store(15)
	collector.cleanupWorkerPendingCount.Store(16)
	collector.cleanupWorkerDurationNanos.Store(17)

	got := cache.Stats()
	if got.EntryCount != 2 || got.MaxEntries != 4 || got.SegmentCount != 2 {
		t.Fatalf("state stats = %+v", got)
	}
	if got.HitCount != 3 || got.MissCount != 7 || got.EvictionCount != 9 || got.ExpirationCount != 11 {
		t.Fatalf("storage counters = %+v", got)
	}
	if got.LoadFoundCount != 13 || got.LoadNotFoundCount != 15 || got.LoadErrorCount != 17 || got.LoadSupersededCount != 19 {
		t.Fatalf("load counters = %+v", got)
	}
	if got.LoadDuration != 2_100*time.Nanosecond {
		t.Fatalf("LoadDuration = %v, want 2100ns", got.LoadDuration)
	}
	if got.SharedCount != 23 || got.DeletedEntryCount != 25 || got.ClearedEntryCount != 13 {
		t.Fatalf("atomic counters = %+v", got)
	}
	if got.CleanupCount != 14 || got.CleanupWorkerRunCount != 15 || got.CleanupWorkerPendingCount != 16 || got.CleanupWorkerDuration != 17*time.Nanosecond {
		t.Fatalf("cleanup counters = %+v", got)
	}
}

func TestStatsRecordLoad(t *testing.T) {
	collector := newStatsCollector(1)
	sentinel := errors.New("load failed")

	collector.recordLoad(0, true, nil, 10*time.Nanosecond)
	collector.recordLoad(0, false, nil, 20*time.Nanosecond)
	collector.recordLoad(0, false, sentinel, 30*time.Nanosecond)
	collector.recordLoadSuperseded(0)

	counters := collector.segment(0)
	if counters.loadFoundCount.Load() != 1 || counters.loadNotFoundCount.Load() != 1 || counters.loadErrorCount.Load() != 1 {
		t.Fatalf(
			"load counts = found:%d notFound:%d error:%d",
			counters.loadFoundCount.Load(),
			counters.loadNotFoundCount.Load(),
			counters.loadErrorCount.Load(),
		)
	}
	if counters.loadSupersededCount.Load() != 1 {
		t.Fatalf("loadSupersededCount = %d, want 1", counters.loadSupersededCount.Load())
	}
	if counters.loadDurationNanos.Load() != 60 {
		t.Fatalf("load duration nanos = %d, want 60", counters.loadDurationNanos.Load())
	}
}

func TestStatsRecordHelpers(t *testing.T) {
	collector := newStatsCollector(1)

	collector.recordShared(0)
	collector.recordDelete(0, 0)
	collector.recordDelete(0, -1)
	collector.recordDelete(0, 3)
	collector.recordClear(0)
	collector.recordClear(-1)
	collector.recordClear(4)
	collector.recordCleanup()
	collector.recordCleanupWorker(false, 10*time.Nanosecond)
	collector.recordCleanupWorker(true, 20*time.Nanosecond)

	if collector.segment(0).sharedCount.Load() != 1 {
		t.Fatalf("sharedCount = %d, want 1", collector.segment(0).sharedCount.Load())
	}
	if collector.segment(0).deletedEntryCount.Load() != 3 {
		t.Fatalf("deletedEntryCount = %d, want 3", collector.segment(0).deletedEntryCount.Load())
	}
	if collector.clearedEntryCount.Load() != 4 {
		t.Fatalf("clearedEntryCount = %d, want 4", collector.clearedEntryCount.Load())
	}
	if collector.cleanupCount.Load() != 1 {
		t.Fatalf("cleanupCount = %d, want 1", collector.cleanupCount.Load())
	}
	if collector.cleanupWorkerRunCount.Load() != 2 || collector.cleanupWorkerPendingCount.Load() != 1 || collector.cleanupWorkerDurationNanos.Load() != 30 {
		t.Fatalf(
			"worker stats = runs:%d pending:%d duration:%d",
			collector.cleanupWorkerRunCount.Load(),
			collector.cleanupWorkerPendingCount.Load(),
			collector.cleanupWorkerDurationNanos.Load(),
		)
	}
}

func TestStatsConcurrentWithCacheOperations(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(128), WithSegmentCount(8))

	var group sync.WaitGroup
	for worker := range 8 {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for iteration := range 1_000 {
				key := string(rune('a' + (worker+iteration)%16))
				cache.Set(key, iteration, NoExpiration)
				cache.Get(key)
				if iteration%11 == 0 {
					cache.Delete(key)
				}
			}
		}(worker)
	}

	group.Add(1)
	go func() {
		defer group.Done()
		for range 1_000 {
			_ = cache.Stats()
		}
	}()

	waitTestGroup(t, &group)
	stats := cache.Stats()
	if stats.MaxEntries != 128 || stats.SegmentCount != 8 {
		t.Fatalf("Stats configuration = %+v", stats)
	}
}
