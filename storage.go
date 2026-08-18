package pacecache

import (
	"hash/maphash"
	"runtime"
	"sync"
	"time"
)

const defaultStorageSegmentCount = 32

type entry[K comparable, V any] struct {
	key K

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
	previous *entry[K, V]
	next     *entry[K, V]

	// Expiration bucket membership. These links are independent from the LRU
	// list so expiration maintenance never needs another per-entry allocation.
	expirationPrevious *entry[K, V]
	expirationNext     *entry[K, V]
}

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

type storageSegment[K comparable, V any] struct {
	mu sync.Mutex

	entries map[K]*entry[K, V]

	head *entry[K, V]
	tail *entry[K, V]

	expirations expirationIndex[K, V]

	slidingExpiration bool

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

// deadlineAt converts an absolute time.Time to this storage's monotonic
// deadline representation. A non-zero time at or before the storage origin is
// represented by a negative deadline so it remains immediately expired rather
// than being confused with NoExpiration.
func (storage *storage[K, V]) deadlineAt(expiresAt time.Time) int64 {
	if storage == nil || expiresAt.IsZero() {
		return 0
	}

	deadline := expiresAt.Sub(storage.origin)
	if deadline <= 0 {
		return -1
	}

	return int64(deadline)
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
) (cachedValue[V], bool) {
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

func (storage *storage[K, V]) getAt(
	index int,
	key K,
	now int64,
	stats *segmentStats,
) (cachedValue[V], bool) {
	return storage.segments[index].get(key, now, stats)
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
	value cachedValue[V],
	deadline int64,
	stats *segmentStats,
) {
	storage.segments[index].set(key, value, deadline, stats)
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

	for {
		remaining := policy.entryBudget
		pending := false

		for index := range storage.segments {
			// Yield after processing the configured entry budget so a large
			// cleanup cannot monopolize the current processor.
			if remaining == 0 {
				runtime.Gosched()
				remaining = policy.entryBudget
			}

			limit := min(policy.batchSize, remaining)

			count, segmentPending := storage.cleanupExpiredAt(index, now, limit, stats.segment(index))

			removed += int64(count)
			remaining -= count

			// A pending segment still contains entries expired at the cleanup
			// snapshot represented by now and requires another round.
			pending = pending || segmentPending
		}

		if !pending {
			return removed
		}

		// Give other goroutines a chance to run before starting another full
		// pass over the segments.
		runtime.Gosched()
	}
}

func (segment *storageSegment[K, V]) exists(
	key K,
	now int64,
	stats *segmentStats,
) bool {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	item, ok := segment.entries[key]
	if !ok {
		return false
	}

	if item.deadline != 0 && now >= item.deadline {
		segment.removeLocked(item)

		if stats != nil {
			stats.expirationCount++
		}

		return false
	}

	return item.found
}

func (segment *storageSegment[K, V]) refreshTTL(
	key K,
	now int64,
	stats *segmentStats,
) bool {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	item, ok := segment.entries[key]
	if !ok {
		return false
	}

	if item.deadline != 0 && now >= item.deadline {
		segment.removeLocked(item)

		if stats != nil {
			stats.expirationCount++
		}

		return false
	}

	if !item.found {
		return false
	}

	if item.refreshTTL > 0 {
		newDeadline := deadlineAfter(now, item.refreshTTL)

		// now may have been captured before waiting for the segment lock.
		// Never let an older refresh shorten a newer deadline.
		if newDeadline > item.deadline {
			segment.expirations.update(item, newDeadline)
		}
	}

	return true
}

func (segment *storageSegment[K, V]) lookup(
	key K,
	now int64,
	stats *segmentStats,
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

func (segment *storageSegment[K, V]) lookupEntry(
	key K,
	now int64,
	stats *segmentStats,
) (cachedEntry[V], bool) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	cached, ok := segment.getEntryLocked(key, now, stats)
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

func (segment *storageSegment[K, V]) get(
	key K,
	now int64,
	stats *segmentStats,
) (cachedValue[V], bool) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	return segment.getLocked(key, now, stats)
}

func (segment *storageSegment[K, V]) getEntry(
	key K,
	now int64,
	stats *segmentStats,
) (cachedEntry[V], bool) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	return segment.getEntryLocked(key, now, stats)
}

func (segment *storageSegment[K, V]) getEntryLocked(
	key K,
	now int64,
	stats *segmentStats,
) (cachedEntry[V], bool) {
	item, ok := segment.entries[key]
	if !ok {
		return cachedEntry[V]{}, false
	}

	// TTL controls logical validity. Once an entry expires, it is no longer
	// considered valid and is removed when observed.
	if item.deadline != 0 && now >= item.deadline {
		segment.removeLocked(item)

		if stats != nil {
			stats.expirationCount++
		}

		return cachedEntry[V]{}, false
	}

	if segment.slidingExpiration && item.refreshTTL > 0 {
		deadline := deadlineAfter(now, item.refreshTTL)

		// now may have been captured before waiting for the segment lock. Never
		// let an older read shorten a deadline established by a newer operation.
		if deadline > item.deadline {
			segment.expirations.update(item, deadline)
		}
	}

	// A live read always updates LRU recency.
	segment.moveToFrontLocked(item)

	return cachedEntry[V]{
		value:    item.value,
		found:    item.found,
		deadline: item.deadline,
	}, true
}

func (segment *storageSegment[K, V]) getLocked(
	key K,
	now int64,
	stats *segmentStats,
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

func (segment *storageSegment[K, V]) set(
	key K,
	cached cachedValue[V],
	deadline int64,
	stats *segmentStats,
) {
	// A zero-capacity segment is possible for direct internal construction with
	// more segments than entries. Such a segment simply stores nothing.
	if segment.maxEntries == 0 {
		return
	}

	// Sliding expiration applies only to positive expiring entries.
	if !cached.found || deadline == 0 {
		cached.refreshTTL = 0
	}

	segment.mu.Lock()
	defer segment.mu.Unlock()

	if item, ok := segment.entries[key]; ok {
		item.value = cached.value
		item.found = cached.found
		item.refreshTTL = cached.refreshTTL

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
		item.value = cached.value
		item.found = cached.found
		item.refreshTTL = cached.refreshTTL

		// remove detaches the victim from the expiration index but deliberately
		// leaves its deadline unchanged. Reset it so update treats the reused
		// entry as having no previous expiration membership.
		item.deadline = 0

		segment.expirations.update(item, deadline)
		segment.entries[key] = item
		segment.moveToFrontLocked(item)

		return
	}

	item := &entry[K, V]{
		key:        key,
		value:      cached.value,
		found:      cached.found,
		refreshTTL: cached.refreshTTL,
	}

	segment.expirations.update(item, deadline)
	segment.entries[key] = item
	segment.pushFrontLocked(item)
}

func (segment *storageSegment[K, V]) delete(key K) bool {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	item, ok := segment.entries[key]
	if !ok {
		return false
	}

	segment.removeLocked(item)

	return true
}

func (segment *storageSegment[K, V]) deleteAll() int64 {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	deleted := int64(len(segment.entries))

	clear(segment.entries)

	segment.head = nil
	segment.tail = nil
	segment.expirations.reset()

	return deleted
}

func (segment *storageSegment[K, V]) removeLocked(item *entry[K, V]) {
	segment.expirations.remove(item)
	delete(segment.entries, item.key)
	segment.unlinkLocked(item)
}

func (segment *storageSegment[K, V]) cleanupExpired(
	now int64,
	limit int,
	stats *segmentStats,
) (int, bool) {
	if limit <= 0 || !segment.expirations.enabled() {
		return 0, false
	}

	segment.mu.Lock()
	defer segment.mu.Unlock()

	dueID := segment.expirations.dueBucketID(now)
	removed := 0

	for removed < limit {
		if !segment.expirations.hasDueBucket(dueID) {
			return removed, false
		}

		item := segment.expirations.popRootEntry()

		// The expiration index and storage map must always reference the same
		// entry. A mismatch indicates an internal consistency bug.
		if segment.entries[item.key] != item {
			panic("pacecache: expiration index is inconsistent with storage")
		}

		delete(segment.entries, item.key)
		segment.unlinkLocked(item)

		removed++

		if stats != nil {
			stats.expirationCount++
		}
	}

	return removed, segment.expirations.hasDueBucket(dueID)
}

func (segment *storageSegment[K, V]) pushFrontLocked(item *entry[K, V]) {
	item.previous = nil
	item.next = segment.head

	if segment.head != nil {
		segment.head.previous = item
	} else {
		segment.tail = item
	}

	segment.head = item
}

func (segment *storageSegment[K, V]) moveToFrontLocked(item *entry[K, V]) {
	if segment.head == item {
		return
	}

	segment.unlinkLocked(item)
	segment.pushFrontLocked(item)
}

func (segment *storageSegment[K, V]) unlinkLocked(item *entry[K, V]) {
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
