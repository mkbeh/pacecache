package pacecache

import "time"

const (
	defaultCleanupBatchSize   = 256
	defaultCleanupEntryBudget = 16 * 1024

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

	pendingSegments []int
	nextSegment     int
}

func newCleanupWorker[V any](
	store *storage[V],
	stats *statsCollector,
	config cleanupConfig,
) *cleanupWorker[V] {
	return &cleanupWorker[V]{
		store:           store,
		stats:           stats,
		config:          config,
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		pendingSegments: make([]int, 0, len(store.segments)),
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

			if worker.cleanupQuantum(worker.store.now()) {
				next = worker.continuationDelay()
			}

			timer.Reset(next)

		case <-worker.stop:
			return
		}
	}
}

// cleanupQuantum performs one bounded cleanupQuantum quantum.
//
// It returns true when there may still be expired entries ready for physical
// removal. The worker reschedules another quantum after a short cooperative
// delay instead of draining an unbounded backlog in one call.
func (worker *cleanupWorker[V]) cleanupQuantum(now int64) bool {
	segmentCount := len(worker.store.segments)
	if segmentCount == 0 {
		return false
	}

	startedAt := time.Now()
	remainingEntries := worker.config.entryBudget

	pendingSegments := worker.pendingSegments[:0]
	startSegment := worker.nextSegment

	for offset := range segmentCount {
		if worker.stopped() {
			worker.pendingSegments = pendingSegments[:0]
			return false
		}

		if remainingEntries == 0 || cleanupTimeBudgetExceeded(startedAt) {
			worker.nextSegment = (startSegment + offset) % segmentCount
			worker.pendingSegments = pendingSegments[:0]
			return true
		}

		segmentIndex := (startSegment + offset) % segmentCount
		batchLimit := min(worker.config.batchSize, remainingEntries)

		removed, hasMore := worker.store.cleanupExpiredAt(
			segmentIndex,
			now,
			batchLimit,
			worker.stats.segment(segmentIndex),
		)

		remainingEntries -= removed

		if hasMore {
			pendingSegments = append(pendingSegments, segmentIndex)
		}
	}

	// Rotate the first segment between complete passes so that no segment gets
	// a permanent first-mover advantage when cleanup repeatedly finds work in
	// many segments.
	worker.nextSegment = (startSegment + 1) % segmentCount

	for len(pendingSegments) != 0 {
		nextCount := 0

		for _, index := range pendingSegments {
			if worker.stopped() {
				worker.pendingSegments = pendingSegments[:0]
				return false
			}

			if remainingEntries == 0 || cleanupTimeBudgetExceeded(startedAt) {
				// Preserve fair continuation by starting the next quantum at the
				// first active segment we did not get to process in this round.
				worker.nextSegment = index
				worker.pendingSegments = pendingSegments[:0]
				return true
			}

			limit := min(worker.config.batchSize, remainingEntries)

			removed, more := worker.store.cleanupExpiredAt(index, now, limit, worker.stats.segment(index))

			remainingEntries -= removed

			if more {
				pendingSegments[nextCount] = index
				nextCount++
			}
		}

		pendingSegments = pendingSegments[:nextCount]
	}

	worker.pendingSegments = pendingSegments[:0]

	return false
}

func (worker *cleanupWorker[V]) continuationDelay() time.Duration {
	return min(worker.config.interval, cleanupContinuationDelay)
}

func (worker *cleanupWorker[V]) stopped() bool {
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
