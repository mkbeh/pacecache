package pacecache

import (
	"sync/atomic"
	"time"
)

// Stats is a detached snapshot of cache statistics.
//
// The snapshot is assembled from independent cache segments. Concurrent cache
// activity may continue while Stats is being collected, so individual fields
// are not guaranteed to represent one globally atomic instant.
type Stats struct {
	// Current state.

	// EntryCount is the number of entries currently resident in storage.
	// Expired entries may remain resident until they are observed, evicted,
	// invalidated, or removed by manual or background cleanup.
	EntryCount int64

	// MaxEntries is the configured total entry budget after applying defaults.
	MaxEntries int64

	// SegmentCount is the number of independent storage and coordination
	// segments.
	SegmentCount int64

	// Lookup lifecycle.

	// HitCount is the cumulative number of positive cache hits.
	HitCount int64

	// NegativeHitCount is the cumulative number of cached negative hits.
	NegativeHitCount int64

	// MissCount is the cumulative number of cache misses. An expired entry
	// observed by a caller is counted as a miss.
	MissCount int64

	// Load lifecycle.

	// LoadFoundCount is the cumulative number of actual loader invocations that
	// returned found=true.
	LoadFoundCount int64

	// LoadNotFoundCount is the cumulative number of actual loader invocations
	// that returned found=false without an error.
	LoadNotFoundCount int64

	// LoadErrorCount is the cumulative number of actual loader invocations that
	// returned an error.
	LoadErrorCount int64

	// LoadSupersededCount is the cumulative number of successful actual loader
	// invocations whose result was discarded because a newer Set, Invalidate, or
	// InvalidateAll operation won before publication.
	//
	// Superseded loads are still included in LoadFoundCount or LoadNotFoundCount,
	// according to the loader result, and are not included in LoadErrorCount.
	LoadSupersededCount int64

	// LoadDuration is the cumulative duration of actual loader invocations,
	// including successful, negative, and failed loads.
	LoadDuration time.Duration

	// SharedCount is the cumulative number of callers that received a
	// singleflight result shared with at least one other caller.
	SharedCount int64

	// Invalidation lifecycle.

	// InvalidatedKeyCount is the cumulative number of resident entries removed by
	// Invalidate. Missing keys and duplicate keys that were already removed do not
	// increase the count.
	InvalidatedKeyCount int64

	// InvalidatedAllCount is the cumulative number of resident entries removed by
	// InvalidateAll.
	//
	// This may include physically resident entries whose TTL had already expired
	// but which had not yet been removed.
	InvalidatedAllCount int64

	// Cleanup lifecycle.

	// CleanupCount is the cumulative number of completed explicit
	// CleanupExpired calls.
	CleanupCount int64

	// CleanupWorkerRunCount is the cumulative number of cleanup quanta
	// completed by the background cleanup worker.
	CleanupWorkerRunCount int64

	// CleanupWorkerPendingCount is the cumulative number of background cleanup
	// quanta that completed with more expired entries still pending.
	CleanupWorkerPendingCount int64

	// CleanupWorkerDuration is the cumulative time spent executing background
	// cleanup quanta. Waiting between cleanup runs is not included.
	CleanupWorkerDuration time.Duration

	// Storage lifecycle.

	// EvictionCount is the cumulative number of entries evicted because a
	// storage segment reached capacity.
	EvictionCount int64

	// ExpirationCount is the cumulative number of entries removed because their
	// TTL expired, either lazily during lookup or by manual or background cleanup.
	ExpirationCount int64
}

// statsCollector owns all mutable cache statistics.
//
// Each segment corresponds to one storage and coordination segment.
type statsCollector struct {
	segments []segmentStats

	invalidatedAllCount atomic.Int64

	cleanupCount               atomic.Int64
	cleanupWorkerRunCount      atomic.Int64
	cleanupWorkerPendingCount  atomic.Int64
	cleanupWorkerDurationNanos atomic.Int64
}

type segmentStats struct {
	// Protected by the corresponding storage segment mutex.
	hitCount         int64
	negativeHitCount int64
	missCount        int64
	evictionCount    int64
	expirationCount  int64

	// Updated outside a suitable existing lock.
	loadFoundCount      atomic.Int64
	loadNotFoundCount   atomic.Int64
	loadErrorCount      atomic.Int64
	loadSupersededCount atomic.Int64
	loadDurationNanos   atomic.Int64

	sharedCount atomic.Int64

	// Sharded to avoid a global hot cache line.
	invalidatedKeyCount atomic.Int64
}

// Stats returns a detached snapshot of the current cache statistics.
func (cache *Cache[K, V]) Stats() Stats {
	if !cache.initialized() {
		return Stats{}
	}

	snapshot := Stats{
		MaxEntries:                int64(cache.store.maxEntries),
		SegmentCount:              int64(len(cache.store.segments)),
		InvalidatedAllCount:       cache.stats.invalidatedAllCount.Load(),
		CleanupCount:              cache.stats.cleanupCount.Load(),
		CleanupWorkerRunCount:     cache.stats.cleanupWorkerRunCount.Load(),
		CleanupWorkerPendingCount: cache.stats.cleanupWorkerPendingCount.Load(),
		CleanupWorkerDuration:     time.Duration(cache.stats.cleanupWorkerDurationNanos.Load()),
	}

	for index := range cache.store.segments {
		segment := &cache.store.segments[index]
		shard := cache.stats.segment(index)

		// Storage counters share the storage segment mutex with the operations
		// that update them.
		segment.mu.Lock()

		snapshot.EntryCount += int64(len(segment.entries))
		snapshot.HitCount += shard.hitCount
		snapshot.NegativeHitCount += shard.negativeHitCount
		snapshot.MissCount += shard.missCount
		snapshot.EvictionCount += shard.evictionCount
		snapshot.ExpirationCount += shard.expirationCount

		segment.mu.Unlock()

		snapshot.LoadFoundCount += shard.loadFoundCount.Load()
		snapshot.LoadNotFoundCount += shard.loadNotFoundCount.Load()
		snapshot.LoadErrorCount += shard.loadErrorCount.Load()
		snapshot.LoadSupersededCount += shard.loadSupersededCount.Load()
		snapshot.LoadDuration += time.Duration(shard.loadDurationNanos.Load())
		snapshot.SharedCount += shard.sharedCount.Load()
		snapshot.InvalidatedKeyCount += shard.invalidatedKeyCount.Load()
	}

	return snapshot
}

func newStatsCollector(segmentCount int) *statsCollector {
	return &statsCollector{
		segments: make([]segmentStats, segmentCount),
	}
}

func (stats *statsCollector) segment(index int) *segmentStats {
	return &stats.segments[index]
}

func (stats *statsCollector) recordLoad(index int, found bool, err error, duration time.Duration) {
	counters := stats.segment(index)

	counters.loadDurationNanos.Add(duration.Nanoseconds())

	switch {
	case err != nil:
		counters.loadErrorCount.Add(1)

	case found:
		counters.loadFoundCount.Add(1)

	default:
		counters.loadNotFoundCount.Add(1)
	}
}

func (stats *statsCollector) recordLoadSuperseded(index int) {
	stats.segment(index).loadSupersededCount.Add(1)
}

func (stats *statsCollector) recordShared(index int) {
	stats.segment(index).sharedCount.Add(1)
}

func (stats *statsCollector) recordKeyInvalidation(index int, count int64) {
	if count <= 0 {
		return
	}

	stats.segment(index).invalidatedKeyCount.Add(count)
}

func (stats *statsCollector) recordAllInvalidation(count int64) {
	if count <= 0 {
		return
	}

	stats.invalidatedAllCount.Add(count)
}

func (stats *statsCollector) recordCleanup() {
	stats.cleanupCount.Add(1)
}

func (stats *statsCollector) recordCleanupWorker(pending bool, duration time.Duration) {
	stats.cleanupWorkerRunCount.Add(1)
	stats.cleanupWorkerDurationNanos.Add(duration.Nanoseconds())

	if pending {
		stats.cleanupWorkerPendingCount.Add(1)
	}
}
