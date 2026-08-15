package pacecache

import (
	"hash/maphash"
	"runtime"
	"sync"
	"time"
)

const defaultStorageSegmentCount = 32

type entry[V any] struct {
	key string

	value V
	found bool

	// refreshTTL stores the effective positive TTL used by sliding expiration.
	// Any configured jitter is selected when the entry is stored and remains
	// stable until the entry is overwritten. Zero means that the entry is not
	// eligible for sliding expiration.
	refreshTTL time.Duration

	// deadline stores the absolute monotonic deadline in nanoseconds relative to
	// storage.origin. Zero means that the entry does not expire.
	deadline int64

	// Exact LRU membership.
	previous *entry[V]
	next     *entry[V]

	// Expiration bucket membership. These links are independent from the LRU
	// list so expiration maintenance never needs another per-entry allocation.
	expirationPrevious *entry[V]
	expirationNext     *entry[V]
}

// storage routes keys across independent storage segments.
//
// Each segment owns its map, LRU list, expiration index, and mutex. Segment
// capacities sum exactly to MaxEntries.
type storage[V any] struct {
	seed   maphash.Seed
	origin time.Time

	segments   []storageSegment[V]
	mask       uint64
	maxEntries int
}

type storageSegment[V any] struct {
	mu sync.Mutex

	entries map[string]*entry[V]

	head *entry[V]
	tail *entry[V]

	expirations expirationIndex[V]

	slidingExpiration bool

	maxEntries int
}

func newStorage[V any](maxEntries, segmentCount int) *storage[V] {
	return newStorageWithExpirationResolution[V](
		maxEntries,
		segmentCount,
		defaultExpirationBucketResolution,
	)
}

func newStorageWithExpirationResolution[V any](
	maxEntries int,
	segmentCount int,
	resolution time.Duration,
) *storage[V] {
	if maxEntries == 0 {
		maxEntries = defaultMaxEntries
	}

	if segmentCount == 0 {
		segmentCount = defaultStorageSegmentCount
	}

	store := newStorageWithSegments[V](maxEntries, segmentCount)
	store.enableExpirationIndex(resolution)

	return store
}

func newStorageWithSegments[V any](maxEntries, segmentCount int) *storage[V] {
	segments := make([]storageSegment[V], segmentCount)

	baseCapacity := maxEntries / segmentCount
	extraCapacity := maxEntries % segmentCount

	for index := range segments {
		capacity := baseCapacity

		if index < extraCapacity {
			capacity++
		}

		segments[index] = storageSegment[V]{
			entries:    make(map[string]*entry[V], capacity),
			maxEntries: capacity,
		}
	}

	mask := uint64(0)

	if segmentCount&(segmentCount-1) == 0 {
		mask = uint64(segmentCount - 1)
	}

	return &storage[V]{
		seed:       maphash.MakeSeed(),
		origin:     time.Now(),
		segments:   segments,
		mask:       mask,
		maxEntries: maxEntries,
	}
}

func (storage *storage[V]) enableExpirationIndex(resolution time.Duration) {
	if resolution <= 0 {
		return
	}

	for index := range storage.segments {
		storage.segments[index].expirations = newExpirationIndex[V](resolution)
	}
}

func (storage *storage[V]) enableSlidingExpiration() {
	for index := range storage.segments {
		storage.segments[index].slidingExpiration = true
	}
}

// now returns monotonic nanoseconds elapsed since this storage was created.
func (storage *storage[V]) now() int64 {
	return int64(time.Since(storage.origin))
}

// elapsedAt converts a time carrying the same monotonic clock domain into
// storage-relative nanoseconds. It is primarily useful when a caller already
// captured a time.Time for another purpose, such as loader duration metrics.
func (storage *storage[V]) elapsedAt(now time.Time) int64 {
	return int64(now.Sub(storage.origin))
}

// deadlineAt converts an absolute time.Time to this storage's monotonic
// deadline representation. A non-zero time at or before the storage origin is
// represented by a negative deadline so it remains immediately expired rather
// than being confused with NoExpiration.
func (storage *storage[V]) deadlineAt(expiresAt time.Time) int64 {
	if storage == nil || expiresAt.IsZero() {
		return 0
	}

	deadline := expiresAt.Sub(storage.origin)
	if deadline <= 0 {
		return -1
	}

	return int64(deadline)
}

func (storage *storage[V]) lookupAt(
	index int,
	key string,
	now int64,
	stats *statsShard,
) (cachedValue[V], bool) {
	return storage.segments[index].lookup(key, now, stats)
}

func (storage *storage[V]) getAt(
	index int,
	key string,
	now int64,
	stats *statsShard,
) (cachedValue[V], bool) {
	return storage.segments[index].get(key, now, stats)
}

func (storage *storage[V]) setAt(
	index int,
	key string,
	value cachedValue[V],
	deadline int64,
	stats *statsShard,
) {
	storage.segments[index].set(key, value, deadline, stats)
}

func (storage *storage[V]) deleteAt(
	index int,
	key string,
) bool {
	return storage.segments[index].delete(key)
}

func (storage *storage[V]) deleteAll() int64 {
	var deleted int64

	for index := range storage.segments {
		deleted += storage.segments[index].deleteAll()
	}

	return deleted
}

func (storage *storage[V]) cleanupExpiredAt(
	index int,
	now int64,
	limit int,
	stats *statsShard,
) (int, bool) {
	return storage.segments[index].cleanupExpired(now, limit, stats)
}

func (storage *storage[V]) cleanupExpired(
	now int64,
	stats *statsCollector,
) int64 {
	var removed int64

	for {
		more := false

		for index := range storage.segments {
			count, pending := storage.cleanupExpiredAt(
				index,
				now,
				cleanupBatchSize,
				stats.shard(index),
			)

			removed += int64(count)
			more = more || pending
		}

		if !more {
			return removed
		}

		runtime.Gosched()
	}
}

func (storage *storage[V]) segmentIndex(key string) int {
	if len(storage.segments) == 1 {
		return 0
	}

	hash := maphash.String(storage.seed, key)

	if storage.mask != 0 {
		return int(hash & storage.mask)
	}

	return int(
		hash % uint64(len(storage.segments)),
	)
}

func (segment *storageSegment[V]) lookup(
	key string,
	now int64,
	stats *statsShard,
) (cachedValue[V], bool) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	cached, ok := segment.getLocked(key, now, stats)
	if !ok {
		if stats != nil {
			stats.missCount++
		}

		return cached, false
	}

	if stats != nil {
		if cached.found {
			stats.hitCount++
		} else {
			stats.negativeHitCount++
		}
	}

	return cached, true
}

func (segment *storageSegment[V]) get(
	key string,
	now int64,
	stats *statsShard,
) (cachedValue[V], bool) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	return segment.getLocked(key, now, stats)
}

func (segment *storageSegment[V]) getLocked(
	key string,
	now int64,
	stats *statsShard,
) (cachedValue[V], bool) {
	item, ok := segment.entries[key]
	if !ok {
		var zero cachedValue[V]

		return zero, false
	}

	// TTL controls logical validity. Once an entry expires, it is no longer
	// considered valid and is removed when observed.
	if item.deadline != 0 && now >= item.deadline {
		segment.removeLocked(item)

		if stats != nil {
			stats.expirationCount++
		}

		var zero cachedValue[V]

		return zero, false
	}

	if segment.slidingExpiration && item.refreshTTL > 0 {
		deadline := deadlineAfter(now, item.refreshTTL)

		// A read may capture now before waiting for the segment lock. Never let a
		// stale timestamp shorten a deadline established by a newer read or Set.
		if deadline > item.deadline {
			segment.expirations.update(item, deadline)
		}
	}

	// A live hit always updates LRU recency. Sliding expiration, when enabled,
	// refreshes positive expiring entries atomically under the same segment lock.
	segment.moveToFrontLocked(item)

	return cachedValue[V]{
		value: item.value,
		found: item.found,
	}, true
}

func (segment *storageSegment[V]) set(
	key string,
	value cachedValue[V],
	deadline int64,
	stats *statsShard,
) {
	// A zero-capacity segment is possible for direct internal construction with
	// more segments than entries. Such a segment simply stores nothing.
	if segment.maxEntries == 0 {
		return
	}

	segment.mu.Lock()
	defer segment.mu.Unlock()

	refreshTTL := value.refreshTTL
	if !value.found || deadline == 0 {
		refreshTTL = 0
	}

	if item, ok := segment.entries[key]; ok {
		item.value = value.value
		item.found = value.found
		item.refreshTTL = refreshTTL
		segment.expirations.update(item, deadline)

		segment.moveToFrontLocked(item)

		return
	}

	// Once the segment reaches capacity, reuse its LRU victim instead of
	// allocating another entry.
	if len(segment.entries) >= segment.maxEntries {
		if stats != nil {
			stats.evictionCount++
		}

		item := segment.tail

		segment.expirations.remove(item)
		delete(segment.entries, item.key)

		item.key = key
		item.value = value.value
		item.found = value.found
		item.refreshTTL = refreshTTL
		item.deadline = 0

		segment.expirations.update(item, deadline)
		segment.entries[key] = item
		segment.moveToFrontLocked(item)

		return
	}

	item := &entry[V]{
		key:        key,
		value:      value.value,
		found:      value.found,
		refreshTTL: refreshTTL,
	}

	segment.expirations.update(item, deadline)
	segment.entries[key] = item
	segment.pushFrontLocked(item)
}

func (segment *storageSegment[V]) delete(key string) bool {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	item, ok := segment.entries[key]
	if !ok {
		return false
	}

	segment.removeLocked(item)

	return true
}

func (segment *storageSegment[V]) deleteAll() int64 {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	deleted := int64(len(segment.entries))

	clear(segment.entries)

	segment.head = nil
	segment.tail = nil
	segment.expirations.reset()

	return deleted
}

func (segment *storageSegment[V]) cleanupExpired(
	now int64,
	limit int,
	stats *statsShard,
) (int, bool) {
	if limit <= 0 || !segment.expirations.enabled() {
		return 0, false
	}

	segment.mu.Lock()
	defer segment.mu.Unlock()

	dueID := segment.expirations.dueBucketID(now)
	removed := 0

	for removed < limit &&
		segment.expirations.rootDue(dueID) {
		item := segment.expirations.popRootEntry()

		current, ok := segment.entries[item.key]
		if !ok || current != item {
			panic("pacecache: expiration index is inconsistent with storage")
		}

		delete(segment.entries, item.key)
		segment.unlinkLocked(item)
		removed++

		if stats != nil {
			stats.expirationCount++
		}
	}

	return removed, segment.expirations.rootDue(dueID)
}

func (segment *storageSegment[V]) removeLocked(item *entry[V]) {
	segment.expirations.remove(item)
	delete(segment.entries, item.key)
	segment.unlinkLocked(item)
}

func (segment *storageSegment[V]) pushFrontLocked(item *entry[V]) {
	item.previous = nil
	item.next = segment.head

	if segment.head != nil {
		segment.head.previous = item
	} else {
		segment.tail = item
	}

	segment.head = item
}

func (segment *storageSegment[V]) moveToFrontLocked(item *entry[V]) {
	if segment.head == item {
		return
	}

	segment.unlinkLocked(item)
	segment.pushFrontLocked(item)
}

func (segment *storageSegment[V]) unlinkLocked(item *entry[V]) {
	if item.previous != nil {
		item.previous.next = item.next
	} else {
		segment.head = item.next
	}

	if item.next != nil {
		item.next.previous = item.previous
	} else {
		segment.tail = item.previous
	}

	item.previous = nil
	item.next = nil
}
