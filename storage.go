package pacecache

import (
	"hash/maphash"
	"sync"
	"time"
)

const defaultStorageSegmentCount = 32

type entry[V any] struct {
	key    string
	cached cachedValue[V]

	expiresAt time.Time

	previous *entry[V]
	next     *entry[V]
}

// storage routes keys across independent storage segments.
//
// Each segment owns its map, LRU list, and mutex. Segment capacities sum
// exactly to MaxEntries.
type storage[V any] struct {
	seed maphash.Seed

	segments   []storageSegment[V]
	mask       uint64
	maxEntries int
}

type storageSegment[V any] struct {
	mu sync.Mutex

	entries map[string]*entry[V]

	head *entry[V]
	tail *entry[V]

	maxEntries int
}

func newStorage[V any](maxEntries, segmentCount int) *storage[V] {
	if maxEntries == 0 {
		maxEntries = defaultMaxEntries
	}

	if segmentCount == 0 {
		segmentCount = defaultStorageSegmentCount
	}

	return newStorageWithSegments[V](maxEntries, segmentCount)
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
		segments:   segments,
		mask:       mask,
		maxEntries: maxEntries,
	}
}

func (storage *storage[V]) lookupAt(
	index int,
	key string,
	now time.Time,
	stats *statsShard,
) (cachedValue[V], bool) {
	return storage.segments[index].lookup(key, now, stats)
}

func (storage *storage[V]) getAt(
	index int,
	key string,
	now time.Time,
	stats *statsShard,
) (cachedValue[V], bool) {
	return storage.segments[index].get(key, now, stats)
}

func (storage *storage[V]) setAt(
	index int,
	key string,
	value cachedValue[V],
	expiresAt time.Time,
	stats *statsShard,
) {
	storage.segments[index].set(key, value, expiresAt, stats)
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
	now time.Time,
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
	now time.Time,
	stats *statsShard,
) (cachedValue[V], bool) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	return segment.getLocked(key, now, stats)
}

func (segment *storageSegment[V]) getLocked(
	key string,
	now time.Time,
	stats *statsShard,
) (cachedValue[V], bool) {
	item, ok := segment.entries[key]
	if !ok {
		var zero cachedValue[V]

		return zero, false
	}

	// TTL controls logical validity. Once an entry expires, it is no longer
	// considered valid and is removed when observed.
	if !item.expiresAt.IsZero() && !now.Before(item.expiresAt) {
		segment.removeLocked(item)

		if stats != nil {
			stats.expirationCount++
		}

		var zero cachedValue[V]

		return zero, false
	}

	// A hit affects LRU recency but never extends the entry TTL.
	segment.moveToFrontLocked(item)

	return item.cached, true
}

func (segment *storageSegment[V]) set(
	key string,
	value cachedValue[V],
	expiresAt time.Time,
	stats *statsShard,
) {
	// A zero-capacity segment is possible when the caller explicitly chooses
	// more segments than MaxEntries. Such a segment simply stores nothing.
	if segment.maxEntries == 0 {
		return
	}

	segment.mu.Lock()
	defer segment.mu.Unlock()

	if item, ok := segment.entries[key]; ok {
		item.cached = value
		item.expiresAt = expiresAt

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

		delete(segment.entries, item.key)

		item.key = key
		item.cached = value
		item.expiresAt = expiresAt

		segment.entries[key] = item

		segment.moveToFrontLocked(item)

		return
	}

	item := &entry[V]{
		key:       key,
		cached:    value,
		expiresAt: expiresAt,
	}

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

	return deleted
}

func (segment *storageSegment[V]) removeLocked(item *entry[V]) {
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
