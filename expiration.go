package pacecache

import (
	"container/heap"
	"time"
)

const (
	defaultExpirationBucketResolution = 5 * time.Second
	maxSpareExpirationBuckets         = 4
)

type expirationIndex[K comparable, V any] struct {
	resolutionNanos int64

	buckets    map[int64]*expirationBucket[K, V]
	bucketHeap expirationBucketHeap[K, V]

	spareBuckets [maxSpareExpirationBuckets]*expirationBucket[K, V]
	spareCount   int
}

type expirationBucket[K comparable, V any] struct {
	id        int64
	heapIndex int
	count     int

	head *entry[K, V]
	tail *entry[K, V]
}

type expirationBucketHeap[K comparable, V any] []*expirationBucket[K, V]

func newExpirationIndex[K comparable, V any](resolution time.Duration) expirationIndex[K, V] {
	return expirationIndex[K, V]{
		resolutionNanos: int64(resolution),
	}
}

func (index *expirationIndex[K, V]) enabled() bool {
	return index != nil && index.resolutionNanos > 0
}

func (index *expirationIndex[K, V]) update(item *entry[K, V], deadline int64) {
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

	oldBucketID := index.bucketID(item.deadline)
	newBucketID := index.bucketID(deadline)

	if oldBucketID == newBucketID {
		item.deadline = deadline
		return
	}

	// If the item is the only member of its bucket and the destination bucket
	// does not exist, reuse the active bucket in place. This keeps the move
	// allocation-free and replaces a heap Remove+Push pair with one heap.Fix.
	if index.rekeySingletonBucket(item, oldBucketID, newBucketID, deadline) {
		return
	}

	index.remove(item)
	item.deadline = deadline
	index.add(item)
}

func (index *expirationIndex[K, V]) add(item *entry[K, V]) {
	if !index.enabled() || item.deadline == 0 {
		return
	}

	id := index.bucketID(item.deadline)
	bucket := index.buckets[id]

	if bucket == nil {
		if index.buckets == nil {
			index.buckets = make(map[int64]*expirationBucket[K, V])
		}

		bucket = index.acquireBucket(id)
		index.buckets[id] = bucket
		heap.Push(&index.bucketHeap, bucket)
	}

	bucket.pushBack(item)
}

func (index *expirationIndex[K, V]) remove(item *entry[K, V]) {
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

func (index *expirationIndex[K, V]) reset() {
	if !index.enabled() {
		return
	}

	active := index.bucketHeap
	index.buckets = nil
	index.bucketHeap = nil

	for _, bucket := range active {
		index.releaseBucket(bucket)
	}
}

func (index *expirationIndex[K, V]) bucketID(deadline int64) int64 {
	if deadline <= 0 {
		return 0
	}

	id := deadline / index.resolutionNanos
	if deadline%index.resolutionNanos != 0 {
		id++
	}

	return id
}

func (index *expirationIndex[K, V]) dueBucketID(now int64) int64 {
	if now < 0 {
		return -1
	}

	return now / index.resolutionNanos
}

func (index *expirationIndex[K, V]) hasDueBucket(dueBucketID int64) bool {
	return len(index.bucketHeap) != 0 && index.bucketHeap[0].id <= dueBucketID
}

func (index *expirationIndex[K, V]) popRootEntry() *entry[K, V] {
	bucket := index.bucketHeap[0]
	item := bucket.popBack()

	if item == nil {
		panic("pacecache: active expiration bucket is empty")
	}

	if bucket.count == 0 {
		delete(index.buckets, bucket.id)
		heap.Pop(&index.bucketHeap)
		index.releaseBucket(bucket)
	}

	return item
}

func (index *expirationIndex[K, V]) entryCount() int64 {
	var total int64

	for _, bucket := range index.bucketHeap {
		total += int64(bucket.count)
	}

	return total
}

func (index *expirationIndex[K, V]) rekeySingletonBucket(
	item *entry[K, V],
	oldBucketID int64,
	newBucketID int64,
	deadline int64,
) bool {
	if index.buckets[newBucketID] != nil {
		return false
	}

	bucket := index.buckets[oldBucketID]
	if bucket == nil ||
		bucket.count != 1 ||
		bucket.head != item ||
		bucket.tail != item ||
		item.expirationPrevious != nil ||
		item.expirationNext != nil ||
		bucket.heapIndex < 0 {
		return false
	}

	delete(index.buckets, oldBucketID)

	bucket.id = newBucketID
	index.buckets[newBucketID] = bucket
	item.deadline = deadline

	heap.Fix(&index.bucketHeap, bucket.heapIndex)

	return true
}

func (index *expirationIndex[K, V]) acquireBucket(id int64) *expirationBucket[K, V] {
	if index.spareCount == 0 {
		return &expirationBucket[K, V]{
			id:        id,
			heapIndex: -1,
		}
	}

	index.spareCount--
	position := index.spareCount
	bucket := index.spareBuckets[position]
	index.spareBuckets[position] = nil

	bucket.id = id

	return bucket
}

func (index *expirationIndex[K, V]) deactivateBucket(bucket *expirationBucket[K, V]) {
	delete(index.buckets, bucket.id)
	heap.Remove(&index.bucketHeap, bucket.heapIndex)
}

func (index *expirationIndex[K, V]) releaseBucket(bucket *expirationBucket[K, V]) {
	bucket.id = 0
	bucket.heapIndex = -1
	bucket.count = 0
	bucket.head = nil
	bucket.tail = nil

	if index.spareCount >= len(index.spareBuckets) {
		return
	}

	position := index.spareCount
	index.spareBuckets[position] = bucket
	index.spareCount++
}

func (bucket *expirationBucket[K, V]) pushBack(item *entry[K, V]) {
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

func (bucket *expirationBucket[K, V]) remove(item *entry[K, V]) {
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

func (bucket *expirationBucket[K, V]) popBack() *entry[K, V] {
	item := bucket.tail
	if item == nil {
		return nil
	}

	bucket.remove(item)

	return item
}

func (queue expirationBucketHeap[K, V]) Len() int {
	return len(queue)
}

func (queue expirationBucketHeap[K, V]) Less(left, right int) bool {
	return queue[left].id < queue[right].id
}

func (queue expirationBucketHeap[K, V]) Swap(left, right int) {
	queue[left], queue[right] = queue[right], queue[left]
	queue[left].heapIndex = left
	queue[right].heapIndex = right
}

func (queue *expirationBucketHeap[K, V]) Push(value any) {
	bucket, ok := value.(*expirationBucket[K, V])
	if !ok {
		panic("pacecache: invalid expiration bucket heap value")
	}

	bucket.heapIndex = len(*queue)
	*queue = append(*queue, bucket)
}

func (queue *expirationBucketHeap[K, V]) Pop() any {
	old := *queue
	last := len(old) - 1
	bucket := old[last]

	old[last] = nil
	bucket.heapIndex = -1
	*queue = old[:last]

	return bucket
}
