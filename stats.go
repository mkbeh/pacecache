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
// Each shard corresponds to one storage and coordination segment.
type statsCollector struct {
	shards              []statsShard
	invalidatedAllCount atomic.Int64
}

type statsShard struct {
	// Protected by the corresponding storage segment mutex.
	hitCount         int64
	negativeHitCount int64
	missCount        int64
	evictionCount    int64
	expirationCount  int64

	// Updated outside a suitable existing lock.
	loadFoundCount    atomic.Int64
	loadNotFoundCount atomic.Int64
	loadErrorCount    atomic.Int64
	loadDurationNanos atomic.Int64

	sharedCount atomic.Int64

	// Sharded to avoid a global hot cache line.
	invalidatedKeyCount atomic.Int64
}

func newStatsCollector(segmentCount int) *statsCollector {
	return &statsCollector{
		shards: make([]statsShard, segmentCount),
	}
}

// Stats returns a detached snapshot of the current cache statistics.
func (cache *Cache[V]) Stats() Stats {
	if !cache.initialized() {
		return Stats{}
	}

	snapshot := Stats{
		MaxEntries:          int64(cache.store.maxEntries),
		SegmentCount:        int64(len(cache.store.segments)),
		InvalidatedAllCount: cache.stats.invalidatedAllCount.Load(),
	}

	for index := range cache.store.segments {
		segment := &cache.store.segments[index]
		shard := cache.stats.shard(index)

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
		snapshot.LoadDuration += time.Duration(shard.loadDurationNanos.Load())
		snapshot.SharedCount += shard.sharedCount.Load()
		snapshot.InvalidatedKeyCount += shard.invalidatedKeyCount.Load()
	}

	return snapshot
}

func (stats *statsCollector) shard(index int) *statsShard {
	return &stats.shards[index]
}

func (stats *statsCollector) recordLoad(index int, found bool, err error, duration time.Duration) {
	shard := stats.shard(index)

	shard.loadDurationNanos.Add(duration.Nanoseconds())

	switch {
	case err != nil:
		shard.loadErrorCount.Add(1)

	case found:
		shard.loadFoundCount.Add(1)

	default:
		shard.loadNotFoundCount.Add(1)
	}
}

func (stats *statsCollector) recordShared(index int) {
	stats.shard(index).sharedCount.Add(1)
}

func (stats *statsCollector) recordKeyInvalidation(index int, count int64) {
	if count <= 0 {
		return
	}

	stats.shard(index).invalidatedKeyCount.Add(count)
}

func (stats *statsCollector) recordAllInvalidation(count int64) {
	if count <= 0 {
		return
	}

	stats.invalidatedAllCount.Add(count)
}
