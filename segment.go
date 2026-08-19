package pacecache

import (
	"sync"
	"time"
)

type entry[K comparable, V any] struct {
	key K

	value V

	// refreshTTL stores the effective TTL used by sliding expiration. Any
	// configured jitter is selected when the entry is stored and remains stable
	// until the entry is overwritten. Zero means that the entry is not eligible
	// for sliding expiration.
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

type storageSegment[K comparable, V any] struct {
	mu sync.Mutex

	entries map[K]*entry[K, V]

	head *entry[K, V]
	tail *entry[K, V]

	expirations expirationIndex[K, V]

	slidingExpiration bool

	maxEntries int
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

	return true
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

	// A live entry without time-based expiration exists, but there is no
	// deadline to refresh.
	if item.refreshTTL == 0 {
		return true
	}

	deadline := deadlineAfter(now, item.refreshTTL)

	// now may have been captured before waiting for the segment lock. Never let
	// an older refresh shorten a deadline established by a newer operation.
	if deadline > item.deadline {
		segment.expirations.update(item, deadline)
	}

	return true
}

func (segment *storageSegment[K, V]) lookup(
	key K,
	now int64,
	stats *segmentStats,
) (V, bool) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	value, ok := segment.getLocked(key, now, stats)
	if !ok {
		if stats != nil {
			stats.missCount++
		}

		return value, false
	}

	if stats != nil {
		stats.hitCount++
	}

	return value, true
}

func (segment *storageSegment[K, V]) lookupEntry(
	key K,
	now int64,
	stats *segmentStats,
) (cachedEntry[V], bool) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	item, ok := segment.entries[key]
	if !ok {
		if stats != nil {
			stats.missCount++
		}

		return cachedEntry[V]{}, false
	}

	// TTL controls logical validity. Once an entry expires, it is no longer
	// considered valid and is removed when observed.
	if item.deadline != 0 && now >= item.deadline {
		segment.removeLocked(item)

		if stats != nil {
			stats.expirationCount++
			stats.missCount++
		}

		return cachedEntry[V]{}, false
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
	// refreshes expiring entries atomically under the same segment lock.
	segment.moveToFrontLocked(item)

	if stats != nil {
		stats.hitCount++
	}

	return cachedEntry[V]{
		value:    item.value,
		deadline: item.deadline,
	}, true
}

func (segment *storageSegment[K, V]) getEntry(
	key K,
	now int64,
	stats *segmentStats,
) (cachedEntry[V], bool) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

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
		deadline: item.deadline,
	}, true
}

func (segment *storageSegment[K, V]) getLocked(
	key K,
	now int64,
	stats *segmentStats,
) (V, bool) {
	item, ok := segment.entries[key]
	if !ok {
		var zero V

		return zero, false
	}

	// TTL controls logical validity. Once an entry expires, it is no longer
	// considered valid and is removed when observed.
	if item.deadline != 0 && now >= item.deadline {
		segment.removeLocked(item)

		if stats != nil {
			stats.expirationCount++
		}

		var zero V

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
	// refreshes expiring entries atomically under the same segment lock.
	segment.moveToFrontLocked(item)

	return item.value, true
}

func (segment *storageSegment[K, V]) set(
	key K,
	value V,
	refreshTTL time.Duration,
	deadline int64,
	stats *segmentStats,
) {
	// A zero-capacity segment is possible for direct internal construction with
	// more segments than entries. Such a segment simply stores nothing.
	if segment.maxEntries == 0 {
		return
	}

	segment.mu.Lock()
	defer segment.mu.Unlock()

	if deadline == 0 {
		refreshTTL = 0
	}

	if item, ok := segment.entries[key]; ok {
		item.value = value
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
		item.value = value
		item.refreshTTL = refreshTTL
		item.deadline = 0

		segment.expirations.update(item, deadline)
		segment.entries[key] = item
		segment.moveToFrontLocked(item)

		return
	}

	item := &entry[K, V]{
		key:        key,
		value:      value,
		refreshTTL: refreshTTL,
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

	for removed < limit &&
		segment.expirations.hasDueBucket(dueID) {
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
