package pacecache

import "time"

const (
	defaultCleanupInterval    = time.Minute
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
	stopCh   chan struct{}

	// scratchSegments is reusable temporary storage for segment indexes that
	// still have due entries after a multi-segment cleanup pass.
	scratchSegments []int
	nextSegment     int
}

func newCleanupWorker[K comparable, V any](
	store *storage[K, V],
	stats *statsCollector,
	policy cleanupPolicy,
	interval time.Duration,
) *cleanupWorker[K, V] {
	worker := &cleanupWorker[K, V]{
		store:    store,
		stats:    stats,
		policy:   policy,
		interval: interval,
		stopCh:   make(chan struct{}),
	}

	if len(store.segments) > 1 {
		worker.scratchSegments = make([]int, 0, len(store.segments))
	}

	return worker
}

func (worker *cleanupWorker[K, V]) run() {
	timer := time.NewTimer(worker.interval)
	defer timer.Stop()

	for {
		select {
		case <-worker.stopCh:
			return

		case <-timer.C:
			select {
			case <-worker.stopCh:
				return
			default:
			}

			cutoff := worker.store.now()
			pending := worker.cleanupQuantum(cutoff)
			worker.stats.recordCleanupWorker(
				pending,
				time.Duration(worker.store.now()-cutoff),
			)

			next := worker.interval
			if pending {
				next = worker.nextDelay()
			}

			timer.Reset(next)
		}
	}
}

// cleanupQuantum performs one bounded cleanup quantum.
//
// It returns true when there may still be expired entries ready for physical
// removal. The worker reschedules another quantum after a short cooperative
// delay instead of draining an unbounded backlog in one call.
func (worker *cleanupWorker[K, V]) cleanupQuantum(cutoff int64) bool {
	if len(worker.store.segments) == 1 {
		return worker.cleanupSingleSegment(cutoff)
	}

	return worker.cleanupSegments(cutoff)
}

func (worker *cleanupWorker[K, V]) cleanupSingleSegment(cutoff int64) bool {
	startedAt := time.Now()
	remaining := worker.policy.entryBudget
	stats := worker.stats.segment(0)

	for {
		if remaining == 0 || cleanupTimeBudgetExceeded(startedAt) {
			return true
		}

		limit := min(worker.policy.batchSize, remaining)
		removed, pending := worker.store.cleanupExpiredAt(0, cutoff, limit, stats)

		remaining -= removed

		if !pending {
			return false
		}
	}
}

func (worker *cleanupWorker[K, V]) cleanupSegments(cutoff int64) bool {
	segmentCount := len(worker.store.segments)
	if segmentCount == 0 {
		return false
	}

	startedAt := time.Now()
	remaining := worker.policy.entryBudget

	pending := worker.scratchSegments[:0]
	start := worker.nextSegment

	for offset := range segmentCount {
		if remaining == 0 || cleanupTimeBudgetExceeded(startedAt) {
			worker.nextSegment = (start + offset) % segmentCount
			worker.scratchSegments = pending[:0]

			return true
		}

		index := (start + offset) % segmentCount
		limit := min(worker.policy.batchSize, remaining)
		stats := worker.stats.segment(index)

		removed, hasMore := worker.store.cleanupExpiredAt(index, cutoff, limit, stats)

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
			if remaining == 0 || cleanupTimeBudgetExceeded(startedAt) {
				// Resume from the first pending segment that this quantum did
				// not get a chance to process.
				worker.nextSegment = index
				worker.scratchSegments = pending[:0]

				return true
			}

			limit := min(worker.policy.batchSize, remaining)
			stats := worker.stats.segment(index)

			removed, hasMore := worker.store.cleanupExpiredAt(index, cutoff, limit, stats)

			remaining -= removed

			if hasMore {
				next = append(next, index)
			}
		}

		pending = next
	}

	worker.scratchSegments = pending[:0]

	return false
}

func (worker *cleanupWorker[K, V]) nextDelay() time.Duration {
	return min(worker.interval, cleanupNextDelay)
}

func cleanupTimeBudgetExceeded(startedAt time.Time) bool {
	return time.Since(startedAt) >= cleanupTimeBudget
}
