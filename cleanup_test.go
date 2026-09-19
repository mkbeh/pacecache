package pacecache

import (
	"runtime"
	"sync"
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

func TestCleanupWorkerEmptyStorage(t *testing.T) {
	store := &storage[string, int]{}
	worker := newTestCleanupWorker(store, newStatsCollector(0), time.Second)
	if worker.cleanupQuantum(0) {
		t.Fatal("empty storage cleanup reported pending work")
	}
}

func TestNewDoesNotStartCleanup(t *testing.T) {
	cache, err := New[string, int](WithCleanupInterval(time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if cache.cleanup != nil {
		t.Fatal("cleanup unexpectedly running after New")
	}
}

func TestStartCleanupRemovesExpiredEntry(t *testing.T) {
	cache, err := New[string, int](
		WithMaxEntries(1),
		WithCleanupInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cache.store.enableExpirationIndex(time.Nanosecond)
	cache.Set("key", 1, time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	done := startTestCleanup(t, cache)

	eventually(t, 500*time.Millisecond, func() bool {
		segment := &cache.store.segments[0]
		segment.mu.Lock()
		defer segment.mu.Unlock()

		return len(segment.entries) == 0
	})

	cache.StopCleanup()
	waitTestSignal(t, done)

	if cache.stats.segment(0).expirationCount != 1 {
		t.Fatalf(
			"expirationCount = %d, want 1",
			cache.stats.segment(0).expirationCount,
		)
	}
	if cache.stats.cleanupWorkerRunCount.Load() == 0 {
		t.Fatal("cleanup worker run was not recorded")
	}
}

func TestStopCleanupWithoutStartIsNoop(t *testing.T) {
	cache := mustNewCache[int](t)

	done := make(chan struct{})
	go func() {
		cache.StopCleanup()
		close(done)
	}()

	waitTestSignal(t, done)
}

func TestStopCleanupStopsRunningWorker(t *testing.T) {
	cache, err := New[string, int](WithCleanupInterval(time.Hour))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	done := startTestCleanup(t, cache)

	cache.StopCleanup()
	waitTestSignal(t, done)

	cache.StopCleanup()
}

func TestStopCleanupConcurrent(t *testing.T) {
	cache, err := New[string, int](WithCleanupInterval(time.Hour))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	done := startTestCleanup(t, cache)

	const callers = 16
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			cache.StopCleanup()
		}()
	}

	waitTestGroup(t, &group)
	waitTestSignal(t, done)
}

func TestStartCleanupCanRestart(t *testing.T) {
	cache, err := New[string, int](WithCleanupInterval(time.Hour))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	firstDone := startTestCleanup(t, cache)
	cache.StopCleanup()
	waitTestSignal(t, firstDone)

	secondDone := startTestCleanup(t, cache)
	cache.StopCleanup()
	waitTestSignal(t, secondDone)
}

func TestStartCleanupReturnsWhenAlreadyRunning(t *testing.T) {
	cache, err := New[string, int](WithCleanupInterval(time.Hour))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	firstDone := startTestCleanup(t, cache)

	secondDone := make(chan struct{})
	go func() {
		cache.StartCleanup()
		close(secondDone)
	}()

	waitTestSignal(t, secondDone)

	cache.StopCleanup()
	waitTestSignal(t, firstDone)
}

func TestCleanupLifecycleNilAndZeroValueSafe(_ *testing.T) {
	var nilCache *Cache[string, int]
	nilCache.StartCleanup()
	nilCache.StopCleanup()

	var zero Cache[string, int]
	zero.StartCleanup()
	zero.StopCleanup()
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
	done := make(chan struct{})
	go func() {
		worker.run()
		close(done)
	}()
	t.Cleanup(func() {
		worker.stopCh <- struct{}{}
		waitTestSignal(t, done)
	})

	eventually(t, time.Second, func() bool {
		return stats.cleanupWorkerPendingCount.Load() > 0 && stats.cleanupWorkerRunCount.Load() > 1
	})
}

func TestCacheCleanupUsesConfiguredLimits(t *testing.T) {
	cache, err := New[string, int](
		WithCleanupInterval(time.Hour),
		WithCleanupBatchSize(7),
		WithCleanupEntryBudget(11),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if cache.cleanupInterval != time.Hour {
		t.Fatalf("cleanup interval = %v, want 1h", cache.cleanupInterval)
	}
	if cache.cleanupPolicy.batchSize != 7 || cache.cleanupPolicy.entryBudget != 11 {
		t.Fatalf(
			"cleanup policy = %+v, want batch=7 budget=11",
			cache.cleanupPolicy,
		)
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

func startTestCleanup[K comparable, V any](
	t *testing.T,
	cache *Cache[K, V],
) <-chan struct{} {
	t.Helper()

	done := make(chan struct{})
	go func() {
		cache.StartCleanup()
		close(done)
	}()

	t.Cleanup(cache.StopCleanup)

	eventually(t, testTimeout, func() bool {
		cache.cleanupMu.Lock()
		defer cache.cleanupMu.Unlock()

		return cache.cleanup != nil
	})

	return done
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
