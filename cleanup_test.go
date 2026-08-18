package pacecache

import (
	"testing"
	"time"
)

func TestNewCleanupWorker(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](8, 2, time.Nanosecond)
	stats := newStatsCollector(2)
	policy := cleanupPolicy{batchSize: 3, entryBudget: 5}
	worker := newCleanupWorker(store, stats, policy, 10*time.Millisecond)

	if worker.store != store {
		t.Fatal("worker.store was not preserved")
	}
	if worker.stats != stats {
		t.Fatal("worker.stats was not preserved")
	}
	if worker.policy != policy {
		t.Fatalf("worker.policy = %+v, want %+v", worker.policy, policy)
	}
	if worker.interval != 10*time.Millisecond {
		t.Fatalf("worker.interval = %v, want %v", worker.interval, 10*time.Millisecond)
	}
	if worker.stop == nil || worker.done == nil {
		t.Fatal("worker channels were not initialized")
	}
	if len(worker.pendingSegments) != 0 || cap(worker.pendingSegments) != len(store.segments) {
		t.Fatalf("pendingSegments = len %d cap %d, want len 0 cap %d", len(worker.pendingSegments), cap(worker.pendingSegments), len(store.segments))
	}
}

func TestCleanupWorkerNextDelay(t *testing.T) {
	worker := &cleanupWorker[string, int]{interval: 10 * time.Millisecond}
	if got := worker.nextDelay(); got != cleanupNextDelay {
		t.Fatalf("nextDelay() = %v, want %v", got, cleanupNextDelay)
	}

	worker.interval = 500 * time.Microsecond
	if got := worker.nextDelay(); got != worker.interval {
		t.Fatalf("nextDelay() = %v, want interval %v", got, worker.interval)
	}
}

func TestCleanupWorkerStopped(t *testing.T) {
	worker := &cleanupWorker[string, int]{stop: make(chan struct{})}
	if worker.stopped() {
		t.Fatal("stopped() = true before stop")
	}

	close(worker.stop)
	if !worker.stopped() {
		t.Fatal("stopped() = false after stop")
	}
}

func TestCleanupTimeBudgetExceeded(t *testing.T) {
	if cleanupTimeBudgetExceeded(time.Now().Add(cleanupTimeBudget)) {
		t.Fatal("future start time exceeded the cleanup budget")
	}
	if !cleanupTimeBudgetExceeded(time.Now().Add(-2 * cleanupTimeBudget)) {
		t.Fatal("old start time did not exceed the cleanup budget")
	}
}

func TestCleanupWorkerQuantum(t *testing.T) {
	t.Run("empty storage", func(t *testing.T) {
		worker := &cleanupWorker[string, int]{
			store: &storage[string, int]{},
			stats: newStatsCollector(0),
		}

		if worker.cleanupQuantum(1) {
			t.Fatal("cleanupQuantum(empty) = true")
		}
	})

	t.Run("entry budget returns continuation", func(t *testing.T) {
		store := newStorageWithExpirationResolution[string, int](4, 1, time.Nanosecond)
		stats := newStatsCollector(1)
		for index := 1; index <= 3; index++ {
			store.setAt(0, string(rune('a'+index)), cachedValue[int]{value: index, found: true}, int64(index), stats.segment(0))
		}

		worker := newCleanupWorker(
			store,
			stats,
			cleanupPolicy{batchSize: 1, entryBudget: 2},
			time.Second,
		)

		if !worker.cleanupQuantum(10) {
			t.Fatal("first cleanupQuantum() = false, want continuation")
		}
		if got := len(store.segments[0].entries); got != 1 {
			t.Fatalf("resident entries after first quantum = %d, want 1", got)
		}
		if len(worker.pendingSegments) != 0 {
			t.Fatalf("pendingSegments len = %d, want 0 reusable slice", len(worker.pendingSegments))
		}

		if worker.cleanupQuantum(10) {
			t.Fatal("second cleanupQuantum() = true, want drained")
		}
		if got := len(store.segments[0].entries); got != 0 {
			t.Fatalf("resident entries after second quantum = %d, want 0", got)
		}
		if got := stats.segment(0).expirationCount; got != 3 {
			t.Fatalf("expirationCount = %d, want 3", got)
		}
	})

	t.Run("rotates and resumes segments", func(t *testing.T) {
		store := newStorageWithExpirationResolution[string, int](4, 2, time.Nanosecond)
		stats := newStatsCollector(2)
		store.setAt(0, "first", cachedValue[int]{value: 1, found: true}, 1, stats.segment(0))
		store.setAt(1, "second", cachedValue[int]{value: 2, found: true}, 1, stats.segment(1))

		worker := newCleanupWorker(
			store,
			stats,
			cleanupPolicy{batchSize: 1, entryBudget: 1},
			time.Second,
		)
		worker.nextSegment = 1

		if !worker.cleanupQuantum(10) {
			t.Fatal("first cleanupQuantum() = false, want continuation")
		}
		if len(store.segments[1].entries) != 0 || len(store.segments[0].entries) != 1 {
			t.Fatal("first quantum did not begin at nextSegment")
		}
		if worker.nextSegment != 0 {
			t.Fatalf("nextSegment = %d, want 0", worker.nextSegment)
		}

		if !worker.cleanupQuantum(10) {
			t.Fatal("second cleanupQuantum() = false, want final continuation after exhausting the budget")
		}
		if len(store.segments[0].entries) != 0 {
			t.Fatal("second quantum did not resume at the remaining segment")
		}
		if worker.cleanupQuantum(10) {
			t.Fatal("third cleanupQuantum() = true, want drained")
		}
	})

	t.Run("stopped worker exits", func(t *testing.T) {
		store := newStorageWithExpirationResolution[string, int](1, 1, time.Nanosecond)
		stats := newStatsCollector(1)
		store.setAt(0, "key", cachedValue[int]{value: 1, found: true}, 1, stats.segment(0))

		worker := newCleanupWorker(
			store,
			stats,
			cleanupPolicy{batchSize: 1, entryBudget: 1},
			time.Second,
		)
		close(worker.stop)

		if worker.cleanupQuantum(10) {
			t.Fatal("cleanupQuantum(stopped) = true")
		}
		if len(store.segments[0].entries) != 1 {
			t.Fatal("stopped worker removed an entry")
		}
	})
}

func TestCleanupWorkerRunRemovesExpiredEntries(t *testing.T) {
	store := newStorageWithExpirationResolution[string, int](2, 1, time.Nanosecond)
	stats := newStatsCollector(1)
	store.setAt(0, "expired", cachedValue[int]{value: 1, found: true}, 1, stats.segment(0))
	store.setAt(0, "live", cachedValue[int]{value: 2, found: true}, 1<<60, stats.segment(0))

	worker := newCleanupWorker(
		store,
		stats,
		cleanupPolicy{batchSize: 1, entryBudget: 1},
		time.Millisecond,
	)
	worker.start()

	waitForCleanupCondition(t, time.Second, func() bool {
		segment := &store.segments[0]
		segment.mu.Lock()
		defer segment.mu.Unlock()

		return segment.entries["expired"] == nil
	})

	worker.close()

	segment := &store.segments[0]
	segment.mu.Lock()
	_, live := segment.entries["live"]
	segment.mu.Unlock()

	if !live {
		t.Fatal("background cleanup removed a live entry")
	}
	if got := stats.segment(0).expirationCount; got != 1 {
		t.Fatalf("expirationCount = %d, want 1", got)
	}
}

func TestCacheCloseStopsCleanupWorker(t *testing.T) {
	cache, err := New[string, int](
		"cleanup",
		WithCleanupInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if cache.cleanup == nil {
		t.Fatal("cleanup worker = nil")
	}

	cache.Close()
	cache.Close()

	select {
	case <-cache.cleanup.done:
	default:
		t.Fatal("cleanup worker is still running after Close")
	}

	cache.Set("key", 1, NoExpiration)
	if value, status := cache.Get("key"); value != 1 || status != LookupHit {
		t.Fatalf("cache after Close = (%d, %v), want (1, LookupHit)", value, status)
	}
}

func waitForCleanupCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatal("condition was not satisfied before timeout")
}
