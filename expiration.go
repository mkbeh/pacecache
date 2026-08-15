package pacecache

import (
	"container/heap"
	"time"
)

const (
	defaultExpirationBucketResolution = 5 * time.Second
	maxSpareExpirationBuckets         = 4
)

type expirationIndex[V any] struct {
	resolution int64

	buckets map[int64]*expirationBucket[V]
	queue   expirationBucketHeap[V]

	spare      [maxSpareExpirationBuckets]*expirationBucket[V]
	spareCount uint8
}

type expirationBucket[V any] struct {
	id        int64
	heapIndex int
	count     int

	head *entry[V]
	tail *entry[V]
}

type expirationBucketHeap[V any] []*expirationBucket[V]

func (queue expirationBucketHeap[V]) Len() int {
	return len(queue)
}

func (queue expirationBucketHeap[V]) Less(left, right int) bool {
	return queue[left].id < queue[right].id
}

func (queue expirationBucketHeap[V]) Swap(left, right int) {
	queue[left], queue[right] = queue[right], queue[left]
	queue[left].heapIndex = left
	queue[right].heapIndex = right
}

func (queue *expirationBucketHeap[V]) Push(value any) {
	bucket, ok := value.(*expirationBucket[V])
	if !ok {
		panic("pacecache: invalid expiration bucket heap value")
	}

	bucket.heapIndex = len(*queue)
	*queue = append(*queue, bucket)
}

func (queue *expirationBucketHeap[V]) Pop() any {
	old := *queue
	last := len(old) - 1
	bucket := old[last]

	old[last] = nil
	bucket.heapIndex = -1
	*queue = old[:last]

	return bucket
}

func newExpirationIndex[V any](resolution time.Duration) expirationIndex[V] {
	return expirationIndex[V]{
		resolution: int64(resolution),
	}
}

func (index *expirationIndex[V]) enabled() bool {
	return index != nil && index.resolution > 0
}

func (index *expirationIndex[V]) update(item *entry[V], deadline int64) {
	if !index.enabled() {
		item.deadline = deadline
		return
	}

	// Entries without time-based expiration are not part of the expiration
	// index. Transitioning from NoExpiration to an expiring entry only needs an
	// ordinary add.
	if item.deadline == 0 {
		item.deadline = deadline
		index.add(item)
		return
	}

	// Transitioning to NoExpiration removes the existing membership and does
	// not create another one.
	if deadline == 0 {
		index.remove(item)
		item.deadline = 0
		return
	}

	oldID := index.bucketID(item.deadline)
	newID := index.bucketID(deadline)

	if oldID == newID {
		item.deadline = deadline
		return
	}

	// If the item is the only member of its bucket and the destination bucket
	// does not exist, reuse the active bucket in place. This keeps the move
	// allocation-free and replaces a heap Remove+Push pair with one heap.Fix.
	if index.rekeySingletonBucket(item, oldID, newID, deadline) {
		return
	}

	index.remove(item)
	item.deadline = deadline
	index.add(item)
}

func (index *expirationIndex[V]) add(item *entry[V]) {
	if !index.enabled() || item.deadline == 0 {
		return
	}

	id := index.bucketID(item.deadline)
	bucket := index.buckets[id]

	if bucket == nil {
		if index.buckets == nil {
			index.buckets = make(map[int64]*expirationBucket[V])
		}

		bucket = index.acquireBucket(id)
		index.buckets[id] = bucket
		heap.Push(&index.queue, bucket)
	}

	bucket.pushBack(item)
}

func (index *expirationIndex[V]) remove(item *entry[V]) {
	if !index.enabled() || item.deadline == 0 {
		return
	}

	id := index.bucketID(item.deadline)
	bucket := index.buckets[id]
	if bucket == nil {
		panic("pacecache: expiration bucket is missing")
	}

	bucket.remove(item)

	if bucket.count == 0 {
		index.deactivateBucket(bucket)
		index.releaseBucket(bucket)
	}
}

func (index *expirationIndex[V]) reset() {
	if !index.enabled() {
		return
	}

	active := index.queue
	index.buckets = nil
	index.queue = nil

	for _, bucket := range active {
		bucket.heapIndex = -1
		index.releaseBucket(bucket)
	}
}

func (index *expirationIndex[V]) dueBucketID(now int64) int64 {
	if now < 0 {
		return -1
	}

	return now / index.resolution
}

func (index *expirationIndex[V]) bucketID(deadline int64) int64 {
	if deadline <= 0 {
		return 0
	}

	id := deadline / index.resolution
	if deadline%index.resolution != 0 {
		id++
	}

	return id
}

func (index *expirationIndex[V]) rootDue(dueID int64) bool {
	return len(index.queue) != 0 && index.queue[0].id <= dueID
}

func (index *expirationIndex[V]) popRootEntry() *entry[V] {
	bucket := index.queue[0]
	item := bucket.popBack()

	if item == nil {
		panic("pacecache: active expiration bucket is empty")
	}

	if bucket.count == 0 {
		delete(index.buckets, bucket.id)
		heap.Pop(&index.queue)
		index.releaseBucket(bucket)
	}

	return item
}

func (index *expirationIndex[V]) entryCount() int64 {
	var total int64

	for _, bucket := range index.queue {
		total += int64(bucket.count)
	}

	return total
}

func (index *expirationIndex[V]) rekeySingletonBucket(
	item *entry[V],
	oldID int64,
	newID int64,
	deadline int64,
) bool {
	if index.buckets[newID] != nil {
		return false
	}

	bucket := index.buckets[oldID]
	if bucket == nil ||
		bucket.count != 1 ||
		bucket.head != item ||
		bucket.tail != item ||
		item.expirationPrevious != nil ||
		item.expirationNext != nil ||
		bucket.heapIndex < 0 {
		return false
	}

	delete(index.buckets, oldID)

	bucket.id = newID
	index.buckets[newID] = bucket
	item.deadline = deadline

	heap.Fix(&index.queue, bucket.heapIndex)

	return true
}

func (index *expirationIndex[V]) acquireBucket(id int64) *expirationBucket[V] {
	if index.spareCount == 0 {
		return &expirationBucket[V]{
			id:        id,
			heapIndex: -1,
		}
	}

	index.spareCount--
	position := int(index.spareCount)
	bucket := index.spare[position]
	index.spare[position] = nil

	bucket.id = id
	bucket.heapIndex = -1
	bucket.count = 0
	bucket.head = nil
	bucket.tail = nil

	return bucket
}

func (index *expirationIndex[V]) deactivateBucket(bucket *expirationBucket[V]) {
	delete(index.buckets, bucket.id)
	heap.Remove(&index.queue, bucket.heapIndex)
}

func (index *expirationIndex[V]) releaseBucket(bucket *expirationBucket[V]) {
	bucket.id = 0
	bucket.heapIndex = -1
	bucket.count = 0
	bucket.head = nil
	bucket.tail = nil

	if index.spareCount >= uint8(len(index.spare)) {
		return
	}

	position := int(index.spareCount)
	index.spare[position] = bucket
	index.spareCount++
}

func (bucket *expirationBucket[V]) pushBack(item *entry[V]) {
	item.expirationPrevious = bucket.tail
	item.expirationNext = nil

	if bucket.tail != nil {
		bucket.tail.expirationNext = item
	} else {
		bucket.head = item
	}

	bucket.tail = item
	bucket.count++
}

func (bucket *expirationBucket[V]) remove(item *entry[V]) {
	if bucket.count <= 0 {
		panic("pacecache: expiration bucket count underflow")
	}

	previous := item.expirationPrevious
	next := item.expirationNext

	if previous != nil {
		previous.expirationNext = next
	} else {
		bucket.head = next
	}

	if next != nil {
		next.expirationPrevious = previous
	} else {
		bucket.tail = previous
	}

	item.expirationPrevious = nil
	item.expirationNext = nil

	bucket.count--
}

func (bucket *expirationBucket[V]) popBack() *entry[V] {
	item := bucket.tail
	if item == nil {
		return nil
	}

	bucket.remove(item)

	return item
}
