package pacecache

import (
	"hash/maphash"
	"runtime"
	"time"
)

const defaultStorageSegmentCount = 32

// storage routes keys across independent storage segments.
//
// Each segment owns its map, LRU list, expiration index, and mutex. Segment
// capacities sum exactly to MaxEntries.
type storage[K comparable, V any] struct {
	seed   maphash.Seed
	origin time.Time

	segments   []storageSegment[K, V]
	mask       uint64
	maxEntries int
}

func newStorage[K comparable, V any](
	maxEntries int,
	segmentCount int,
	slidingExpiration bool,
) *storage[K, V] {
	store := newStorageWithExpirationResolution[K, V](
		maxEntries,
		segmentCount,
		defaultExpirationBucketResolution,
	)

	if slidingExpiration {
		store.enableSlidingExpiration()
	}

	return store
}

func newStorageWithExpirationResolution[K comparable, V any](
	maxEntries int,
	segmentCount int,
	resolution time.Duration,
) *storage[K, V] {
	if maxEntries == 0 {
		maxEntries = defaultMaxEntries
	}

	if segmentCount == 0 {
		segmentCount = defaultStorageSegmentCount
	}

	store := newStorageWithSegments[K, V](maxEntries, segmentCount)
	store.enableExpirationIndex(resolution)

	return store
}

func newStorageWithSegments[K comparable, V any](maxEntries, segmentCount int) *storage[K, V] {
	segments := make([]storageSegment[K, V], segmentCount)

	baseCapacity := maxEntries / segmentCount
	extraCapacity := maxEntries % segmentCount

	for index := range segments {
		capacity := baseCapacity

		if index < extraCapacity {
			capacity++
		}

		segments[index] = storageSegment[K, V]{
			entries:    make(map[K]*entry[K, V], capacity),
			maxEntries: capacity,
		}
	}

	mask := uint64(0)

	if segmentCount&(segmentCount-1) == 0 {
		mask = uint64(segmentCount - 1)
	}

	return &storage[K, V]{
		seed:       maphash.MakeSeed(),
		origin:     time.Now(),
		segments:   segments,
		mask:       mask,
		maxEntries: maxEntries,
	}
}

func (storage *storage[K, V]) enableExpirationIndex(resolution time.Duration) {
	if resolution <= 0 {
		return
	}

	for index := range storage.segments {
		storage.segments[index].expirations = newExpirationIndex[K, V](resolution)
	}
}

func (storage *storage[K, V]) enableSlidingExpiration() {
	for index := range storage.segments {
		storage.segments[index].slidingExpiration = true
	}
}

// now returns monotonic nanoseconds elapsed since this storage was created.
func (storage *storage[K, V]) now() int64 {
	return int64(time.Since(storage.origin))
}

// expiresAt converts a storage-relative monotonic deadline into time.Time.
// A zero deadline represents an entry without time-based expiration.
func (storage *storage[K, V]) expiresAt(deadline int64) time.Time {
	if storage == nil || deadline == 0 {
		return time.Time{}
	}

	return storage.origin.Add(time.Duration(deadline))
}

func (storage *storage[K, V]) segmentIndex(key K) int {
	if len(storage.segments) == 1 {
		return 0
	}

	hash := maphash.Comparable(storage.seed, key)

	if storage.mask != 0 {
		return int(hash & storage.mask)
	}

	return int(
		hash % uint64(len(storage.segments)),
	)
}

func (storage *storage[K, V]) existsAt(
	index int,
	key K,
	now int64,
	stats *segmentStats,
) bool {
	return storage.segments[index].exists(key, now, stats)
}

func (storage *storage[K, V]) refreshTTLAt(
	index int,
	key K,
	now int64,
	stats *segmentStats,
) bool {
	return storage.segments[index].refreshTTL(key, now, stats)
}

func (storage *storage[K, V]) lookupAt(
	index int,
	key K,
	now int64,
	stats *segmentStats,
) (V, bool) {
	return storage.segments[index].lookup(key, now, stats)
}

func (storage *storage[K, V]) lookupEntryAt(
	index int,
	key K,
	now int64,
	stats *segmentStats,
) (cachedEntry[V], bool) {
	return storage.segments[index].lookupEntry(key, now, stats)
}

func (storage *storage[K, V]) getEntryAt(
	index int,
	key K,
	now int64,
	stats *segmentStats,
) (cachedEntry[V], bool) {
	return storage.segments[index].getEntry(key, now, stats)
}

func (storage *storage[K, V]) setAt(
	index int,
	key K,
	value V,
	refreshTTL time.Duration,
	deadline int64,
	stats *segmentStats,
) {
	storage.segments[index].set(key, value, refreshTTL, deadline, stats)
}

func (storage *storage[K, V]) getOrSetAt(
	index int,
	key K,
	value V,
	refreshTTL time.Duration,
	deadline int64,
	now int64,
	stats *segmentStats,
) (V, bool) {
	return storage.segments[index].getOrSet(
		key,
		value,
		refreshTTL,
		deadline,
		now,
		stats,
	)
}

func (storage *storage[K, V]) getOrSetEntryAt(
	index int,
	key K,
	value V,
	refreshTTL time.Duration,
	deadline int64,
	now int64,
	stats *segmentStats,
) (cachedEntry[V], bool) {
	return storage.segments[index].getOrSetEntry(
		key,
		value,
		refreshTTL,
		deadline,
		now,
		stats,
	)
}

func (storage *storage[K, V]) getAndDeleteAt(
	index int,
	key K,
	now int64,
	stats *segmentStats,
) (V, bool) {
	return storage.segments[index].getAndDelete(key, now, stats)
}

func (storage *storage[K, V]) deleteAt(
	index int,
	key K,
) bool {
	return storage.segments[index].delete(key)
}

func (storage *storage[K, V]) deleteAll() int64 {
	var deleted int64

	for index := range storage.segments {
		deleted += storage.segments[index].deleteAll()
	}

	return deleted
}

func (storage *storage[K, V]) cleanupExpiredAt(
	index int,
	now int64,
	limit int,
	stats *segmentStats,
) (int, bool) {
	return storage.segments[index].cleanupExpired(now, limit, stats)
}

func (storage *storage[K, V]) cleanupExpired(
	now int64,
	policy cleanupPolicy,
	stats *statsCollector,
) int64 {
	var removed int64

	remainingEntries := policy.entryBudget

	for {
		hasMore := false

		for index := range storage.segments {
			if remainingEntries == 0 {
				runtime.Gosched()
				remainingEntries = policy.entryBudget
			}

			batchLimit := min(policy.batchSize, remainingEntries)

			count, pending := storage.cleanupExpiredAt(
				index,
				now,
				batchLimit,
				stats.segment(index),
			)

			removed += int64(count)
			remainingEntries -= count
			hasMore = hasMore || pending
		}

		if !hasMore {
			return removed
		}

		runtime.Gosched()
		remainingEntries = policy.entryBudget
	}
}
