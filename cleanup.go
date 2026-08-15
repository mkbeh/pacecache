package pacecache

import "time"

const (
	cleanupBatchSize         = 256
	cleanupEntryBudget       = 16 * 1024
	cleanupTimeBudget        = time.Millisecond
	cleanupContinuationDelay = time.Millisecond
)

type cleanupConfig struct {
	interval    time.Duration
	batchSize   int
	entryBudget int
}

type cleanupWorker[V any] struct {
	store *storage[V]
	stats *statsCollector

	config cleanupConfig
	stop   chan struct{}
	done   chan struct{}

	active []int
	cursor int
}

func newCleanupWorker[V any](
	store *storage[V],
	stats *statsCollector,
	config cleanupConfig,
) *cleanupWorker[V] {
	return &cleanupWorker[V]{
		store:  store,
		stats:  stats,
		config: config,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		active: make([]int, 0, len(store.segments)),
	}
}

func (worker *cleanupWorker[V]) start() {
	go worker.run()
}

func (worker *cleanupWorker[V]) close() {
	close(worker.stop)
	<-worker.done
}

func (worker *cleanupWorker[V]) run() {
	timer := time.NewTimer(worker.config.interval)
	defer timer.Stop()
	defer close(worker.done)

	for {
		select {
		case <-timer.C:
			next := worker.config.interval

			if worker.cleanup(worker.store.now()) {
				next = worker.continuationDelay()
			}

			timer.Reset(next)

		case <-worker.stop:
			return
		}
	}
}

// cleanup performs one bounded cleanup quantum.
//
// It returns true when there may still be expired entries ready for physical
// removal. The worker reschedules another quantum after a short cooperative
// delay instead of draining an unbounded backlog in one call.
func (worker *cleanupWorker[V]) cleanup(now int64) bool {
	segmentCount := len(worker.store.segments)
	if segmentCount == 0 {
		return false
	}

	startedAt := time.Now()
	remaining := worker.config.entryBudget

	active := worker.active[:0]
	start := worker.cursor

	for offset := range segmentCount {
		if worker.stopped() {
			worker.active = active[:0]
			return false
		}

		if remaining == 0 || cleanupBudgetExpired(startedAt) {
			worker.cursor = (start + offset) % segmentCount
			worker.active = active[:0]
			return true
		}

		index := (start + offset) % segmentCount
		limit := min(worker.config.batchSize, remaining)

		removed, more := worker.store.cleanupExpiredAt(index, now, limit, worker.stats.shard(index))

		remaining -= removed

		if more {
			active = append(active, index)
		}
	}

	// Rotate the first segment between complete passes so that no segment gets
	// a permanent first-mover advantage when cleanup repeatedly finds work in
	// many segments.
	worker.cursor = (start + 1) % segmentCount

	for len(active) != 0 {
		nextCount := 0

		for _, index := range active {
			if worker.stopped() {
				worker.active = active[:0]
				return false
			}

			if remaining == 0 || cleanupBudgetExpired(startedAt) {
				// Preserve fair continuation by starting the next quantum at the
				// first active segment we did not get to process in this round.
				worker.cursor = index
				worker.active = active[:0]
				return true
			}

			limit := min(worker.config.batchSize, remaining)

			removed, more := worker.store.cleanupExpiredAt(index, now, limit, worker.stats.shard(index))

			remaining -= removed

			if more {
				active[nextCount] = index
				nextCount++
			}
		}

		active = active[:nextCount]
	}

	worker.active = active[:0]

	return false
}

func (worker *cleanupWorker[V]) continuationDelay() time.Duration {
	return min(worker.config.interval, cleanupContinuationDelay)
}

func cleanupBudgetExpired(startedAt time.Time) bool {
	return time.Since(startedAt) >= cleanupTimeBudget
}

func (worker *cleanupWorker[V]) stopped() bool {
	select {
	case <-worker.stop:
		return true
	default:
		return false
	}
}
