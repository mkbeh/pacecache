package pacecache

import "time"

const (
	defaultCleanupBatchSize   = 256
	defaultCleanupEntryBudget = 16 * 1024

	cleanupTimeBudget = time.Millisecond
	cleanupNextDelay  = time.Millisecond
)

type cleanupPolicy struct {
	batchSize   int
	entryBudget int
}

type cleanupWorker[K comparable, V any] struct {
	store *storage[K, V]
	stats *statsCollector

	policy   cleanupPolicy
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}

	pendingSegments []int
	nextSegment     int
}

func newCleanupWorker[K comparable, V any](
	store *storage[K, V],
	stats *statsCollector,
	policy cleanupPolicy,
	interval time.Duration,
) *cleanupWorker[K, V] {
	return &cleanupWorker[K, V]{
		store:           store,
		stats:           stats,
		policy:          policy,
		interval:        interval,
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		pendingSegments: make([]int, 0, len(store.segments)),
	}
}

func (worker *cleanupWorker[K, V]) start() {
	go worker.run()
}

func (worker *cleanupWorker[K, V]) close() {
	close(worker.stop)
	<-worker.done
}

func (worker *cleanupWorker[K, V]) run() {
	timer := time.NewTimer(worker.interval)
	defer timer.Stop()
	defer close(worker.done)

	for {
		select {
		case <-timer.C:
			next := worker.interval

			if worker.cleanupQuantum(worker.store.now()) {
				next = worker.nextDelay()
			}

			timer.Reset(next)

		case <-worker.stop:
			return
		}
	}
}

// cleanupQuantum performs one bounded cleanup quantum.
//
// It returns true when there may still be expired entries ready for physical
// removal. The worker reschedules another quantum after a short cooperative
// delay instead of draining an unbounded backlog in one call.
func (worker *cleanupWorker[K, V]) cleanupQuantum(now int64) bool {
	segmentCount := len(worker.store.segments)
	if segmentCount == 0 {
		return false
	}

	startedAt := time.Now()
	remaining := worker.policy.entryBudget

	pending := worker.pendingSegments[:0]
	start := worker.nextSegment

	for offset := range segmentCount {
		if worker.stopped() {
			worker.pendingSegments = pending[:0]
			return false
		}

		if remaining == 0 || cleanupTimeBudgetExceeded(startedAt) {
			worker.nextSegment = (start + offset) % segmentCount
			worker.pendingSegments = pending[:0]

			return true
		}

		index := (start + offset) % segmentCount
		limit := min(worker.policy.batchSize, remaining)
		stats := worker.stats.segment(index)

		removed, hasMore := worker.store.cleanupExpiredAt(index, now, limit, stats)

		remaining -= removed

		if hasMore {
			pending = append(pending, index)
		}
	}

	// Rotate the first segment between complete passes so no segment gets a
	// permanent first-mover advantage when many segments repeatedly have work.
	worker.nextSegment = (start + 1) % segmentCount

	for len(pending) != 0 {
		next := pending[:0]

		for _, index := range pending {
			if worker.stopped() {
				worker.pendingSegments = pending[:0]
				return false
			}

			if remaining == 0 || cleanupTimeBudgetExceeded(startedAt) {
				// Resume from the first pending segment that this quantum did
				// not get a chance to process.
				worker.nextSegment = index
				worker.pendingSegments = pending[:0]

				return true
			}

			limit := min(worker.policy.batchSize, remaining)
			stats := worker.stats.segment(index)

			removed, hasMore := worker.store.cleanupExpiredAt(index, now, limit, stats)

			remaining -= removed

			if hasMore {
				next = append(next, index)
			}
		}

		pending = next
	}

	worker.pendingSegments = pending[:0]

	return false
}

func (worker *cleanupWorker[K, V]) nextDelay() time.Duration {
	return min(worker.interval, cleanupNextDelay)
}

func (worker *cleanupWorker[K, V]) stopped() bool {
	select {
	case <-worker.stop:
		return true
	default:
		return false
	}
}

func cleanupTimeBudgetExceeded(startedAt time.Time) bool {
	return time.Since(startedAt) >= cleanupTimeBudget
}
