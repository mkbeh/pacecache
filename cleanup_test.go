package pacecache

import (
	"runtime"
	"testing"
	"time"
)

func newTestCleanupWorker[V any](
	store *storage[string, V],
	stats *statsCollector,
	interval time.Duration,
) *cleanupWorker[string, V] {
	return newCleanupWorker(
		store,
		stats,
		cleanupPolicy{
			batchSize:   defaultCleanupBatchSize,
			entryBudget: defaultCleanupEntryBudget,
		},
		interval,
	)
}

func TestCleanupWorkerNextDelay(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](1, 1, time.Nanosecond)
	stats := newStatsCollector(1)

	worker := newTestCleanupWorker(store, stats, 10*time.Millisecond)
	if got := worker.nextDelay(); got != cleanupNextDelay {
		t.Fatalf("nextDelay = %v, want %v", got, cleanupNextDelay)
	}

	fastWorker := newTestCleanupWorker(store, stats, 500*time.Microsecond)
	if got := fastWorker.nextDelay(); got != 500*time.Microsecond {
		t.Fatalf("nextDelay = %v, want 500us", got)
	}
}

func TestCleanupWorkerStopped(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](1, 1, time.Nanosecond)
	worker := newTestCleanupWorker(store, newStatsCollector(1), time.Second)

	if worker.stopped() {
		t.Fatal("new worker unexpectedly stopped")
	}
	close(worker.stop)
	if !worker.stopped() {
		t.Fatal("closed worker stop channel not observed")
	}
	if worker.cleanupQuantum(10) {
		t.Fatal("cleanup reported pending work after stop")
	}
}

func TestCleanupWorkerEmptyStorage(t *testing.T) {
	store := &storage[string, int]{}
	worker := newTestCleanupWorker(store, newStatsCollector(0), time.Second)
	if worker.cleanupQuantum(0) {
		t.Fatal("empty storage cleanup reported pending work")
	}
}

func TestCleanupWorkerBackgroundRemovesExpiredEntry(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](1, 1, time.Millisecond)
	stats := newStatsCollector(1)
	now := store.now()
	store.setAt(0, "key", 1, 2*time.Millisecond, deadlineAfter(now, 2*time.Millisecond), stats.segment(0))

	worker := newTestCleanupWorker(store, stats, time.Millisecond)
	worker.start()
	t.Cleanup(worker.close)

	eventually(t, 500*time.Millisecond, func() bool {
		segment := &store.segments[0]
		segment.mu.Lock()
		defer segment.mu.Unlock()
		return len(segment.entries) == 0
	})

	if stats.segment(0).expirationCount != 1 {
		t.Fatalf("expirationCount = %d, want 1", stats.segment(0).expirationCount)
	}
	if stats.cleanupWorkerRunCount.Load() == 0 {
		t.Fatal("cleanup worker run was not recorded")
	}
}

func TestCacheCloseStopsBackgroundCleanup(t *testing.T) {
	cache, err := New[string, int]("users", WithCleanupInterval(time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if cache.cleanup == nil {
		t.Fatal("cleanup worker is nil")
	}

	cache.Close()
	waitTestSignal(t, cache.cleanup.done)
	cache.Close()
}

func TestCleanupWorkerDrainsActiveSegmentAcrossBatches(t *testing.T) {
	const entries = defaultCleanupBatchSize*2 + 17
	store := newStorageWithExpirationResolution[string, int](entries, 1, time.Nanosecond)
	stats := newStatsCollector(1)

	for index := range entries {
		key := string(rune(index + 1))
		store.setAt(0, key, index, time.Nanosecond, 1, stats.segment(0))
	}

	worker := newTestCleanupWorker(store, stats, time.Second)
	if pending := worker.cleanupQuantum(2); pending {
		t.Fatal("cleanup reported pending after draining bounded backlog")
	}
	if len(store.segments[0].entries) != 0 {
		t.Fatalf("resident entries = %d, want 0", len(store.segments[0].entries))
	}
	if stats.segment(0).expirationCount != entries {
		t.Fatalf("expirationCount = %d, want %d", stats.segment(0).expirationCount, entries)
	}
}

func TestCleanupWorkerReturnsContinuationForLargeBacklog(t *testing.T) {
	const entries = defaultCleanupEntryBudget * 2
	store := newStorageWithExpirationResolution[string, int](entries, 1, time.Nanosecond)
	stats := newStatsCollector(1)

	for index := range entries {
		key := string(rune(index + 1))
		store.setAt(0, key, index, time.Nanosecond, 1, stats.segment(0))
	}

	worker := newTestCleanupWorker(store, stats, time.Second)
	if pending := worker.cleanupQuantum(2); !pending {
		t.Fatal("large backlog cleanup returned false, want continuation")
	}

	segment := &store.segments[0]
	segment.mu.Lock()
	remaining := len(segment.entries)
	segment.mu.Unlock()
	if remaining == 0 || remaining >= entries {
		t.Fatalf("remaining entries = %d, want partial progress", remaining)
	}
}

func TestCleanupWorkerEntryBudgetStopsQuantumDeterministically(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 2, time.Nanosecond)
	stats := newStatsCollector(2)

	for index := range 2 {
		store.setAt(index, string(rune('a'+index)), index, time.Nanosecond, 1, stats.segment(index))
	}

	worker := newCleanupWorker(
		store,
		stats,
		cleanupPolicy{batchSize: 1, entryBudget: 1},
		time.Second,
	)

	if pending := worker.cleanupQuantum(2); !pending {
		t.Fatal("cleanup returned false, want continuation after entry budget")
	}
	if got := stats.segment(0).expirationCount + stats.segment(1).expirationCount; got != 1 {
		t.Fatalf("removed entries = %d, want exactly 1", got)
	}
}

func TestCleanupWorkerRunSchedulesContinuationForBacklog(t *testing.T) {
	const entries = defaultCleanupEntryBudget * 2
	store := newStorageWithExpirationResolution[string, int](entries, 1, time.Nanosecond)
	stats := newStatsCollector(1)

	for index := range entries {
		key := string(rune(index + 1))
		store.setAt(0, key, index, time.Nanosecond, 1, stats.segment(0))
	}

	worker := newTestCleanupWorker(store, stats, time.Millisecond)
	worker.start()
	t.Cleanup(worker.close)

	eventually(t, time.Second, func() bool {
		return stats.cleanupWorkerPendingCount.Load() > 0 && stats.cleanupWorkerRunCount.Load() > 1
	})
}

func TestCacheCleanupWorkerUsesConfiguredLimits(t *testing.T) {
	cache, err := New[string, int](
		"users",
		WithCleanupInterval(time.Hour),
		WithCleanupBatchSize(7),
		WithCleanupEntryBudget(11),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(cache.Close)

	if cache.cleanup == nil {
		t.Fatal("cleanup worker is nil")
	}
	if cache.cleanup.policy.batchSize != 7 || cache.cleanup.policy.entryBudget != 11 {
		t.Fatalf("cleanup policy = %+v, want batch=7 budget=11", cache.cleanup.policy)
	}
}

func TestCleanupWorkerHonorsConfiguredEntryBudget(t *testing.T) {
	const entries = 5
	store := newStorageWithExpirationResolution[string, int](entries, 1, time.Nanosecond)
	stats := newStatsCollector(1)

	for index := range entries {
		key := string(rune(index + 1))
		store.setAt(0, key, index, time.Nanosecond, 1, stats.segment(0))
	}

	worker := newCleanupWorker(
		store,
		stats,
		cleanupPolicy{batchSize: 2, entryBudget: 3},
		time.Second,
	)

	if pending := worker.cleanupQuantum(2); !pending {
		t.Fatal("cleanup returned false, want continuation")
	}

	segment := &store.segments[0]
	segment.mu.Lock()
	remaining := len(segment.entries)
	segment.mu.Unlock()

	if remaining != 2 {
		t.Fatalf("remaining entries = %d, want 2", remaining)
	}
	if stats.segment(0).expirationCount != 3 {
		t.Fatalf("expirationCount = %d, want 3", stats.segment(0).expirationCount)
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}

	if !condition() {
		t.Fatal("condition was not satisfied before timeout")
	}
}
