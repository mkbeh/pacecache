package pacecache

import (
	"container/heap"
	"testing"
	"time"
)

func TestExpirationBucketIDAndDueBucketID(t *testing.T) {
	index := newExpirationIndex[string, int](10 * time.Nanosecond)

	tests := []struct {
		deadline int64
		want     int64
	}{
		{deadline: -1, want: 0},
		{deadline: 0, want: 0},
		{deadline: 1, want: 1},
		{deadline: 10, want: 1},
		{deadline: 11, want: 2},
		{deadline: 20, want: 2},
	}
	for _, test := range tests {
		if got := index.bucketID(test.deadline); got != test.want {
			t.Fatalf("bucketID(%d) = %d, want %d", test.deadline, got, test.want)
		}
	}

	if got := index.dueBucketID(-1); got != -1 {
		t.Fatalf("dueBucketID(-1) = %d, want -1", got)
	}
	if got := index.dueBucketID(0); got != 0 {
		t.Fatalf("dueBucketID(0) = %d, want 0", got)
	}
	if got := index.dueBucketID(19); got != 1 {
		t.Fatalf("dueBucketID(19) = %d, want 1", got)
	}
}

func TestExpirationIndexDisabled(t *testing.T) {
	var index expirationIndex[string, int]
	item := &entry[string, int]{}

	if index.enabled() {
		t.Fatal("zero expiration index unexpectedly enabled")
	}
	index.update(item, 100)
	if item.deadline != 100 {
		t.Fatalf("deadline = %d, want 100", item.deadline)
	}
	index.add(item)
	index.remove(item)
	index.reset()
	if len(index.bucketHeap) != 0 || index.buckets != nil {
		t.Fatal("disabled index created membership")
	}
}

func TestExpirationIndexUpdateTransitions(t *testing.T) {
	index := newExpirationIndex[string, int](10 * time.Nanosecond)
	item := &entry[string, int]{key: "key"}

	index.update(item, 5)
	if item.deadline != 5 || expirationIndexEntryCount(&index) != 1 || len(index.bucketHeap) != 1 {
		t.Fatalf("after initial add: deadline=%d entries=%d queue=%d", item.deadline, expirationIndexEntryCount(&index), len(index.bucketHeap))
	}
	originalBucket := index.bucketHeap[0]

	// Same bucket only updates the deadline.
	index.update(item, 9)
	if item.deadline != 9 || index.bucketHeap[0] != originalBucket || expirationIndexEntryCount(&index) != 1 {
		t.Fatal("same-bucket update changed membership")
	}

	// Singleton move rekeys the existing bucket in place.
	index.update(item, 25)
	if item.deadline != 25 || index.bucketHeap[0] != originalBucket || originalBucket.id != 3 {
		t.Fatal("singleton bucket was not rekeyed in place")
	}

	index.update(item, 0)
	if item.deadline != 0 || expirationIndexEntryCount(&index) != 0 || len(index.bucketHeap) != 0 {
		t.Fatalf("transition to no-expiration left membership: deadline=%d entries=%d queue=%d", item.deadline, expirationIndexEntryCount(&index), len(index.bucketHeap))
	}
}

func TestExpirationIndexMovesBetweenExistingBuckets(t *testing.T) {
	index := newExpirationIndex[string, int](10 * time.Nanosecond)
	first := &entry[string, int]{key: "first"}
	second := &entry[string, int]{key: "second"}

	index.update(first, 5)   // bucket 1
	index.update(second, 25) // bucket 3
	oldBucket := index.buckets[1]
	destination := index.buckets[3]

	index.update(first, 25)
	if expirationIndexEntryCount(&index) != 2 {
		t.Fatalf("entryCount = %d, want 2", expirationIndexEntryCount(&index))
	}
	if index.buckets[3] != destination || destination.count != 2 {
		t.Fatal("entry was not moved into existing destination bucket")
	}
	if index.buckets[1] != nil {
		t.Fatal("empty source bucket remains active")
	}
	if oldBucket.id != 0 {
		t.Fatal("released source bucket was not reset")
	}
}

func TestExpirationIndexPopRootEntry(t *testing.T) {
	index := newExpirationIndex[string, int](10 * time.Nanosecond)
	first := &entry[string, int]{key: "first"}
	second := &entry[string, int]{key: "second"}
	third := &entry[string, int]{key: "third"}

	index.update(first, 5)
	index.update(second, 5)
	index.update(third, 25)

	if !index.hasDueBucket(1) {
		t.Fatal("rootDue(1) = false, want true")
	}
	if index.hasDueBucket(0) {
		t.Fatal("rootDue(0) = true, want false")
	}

	popped := index.popRootEntry()
	if popped != second {
		t.Fatalf("first popped = %q, want second (bucket tail)", popped.key)
	}
	if expirationIndexEntryCount(&index) != 2 {
		t.Fatalf("entryCount = %d, want 2", expirationIndexEntryCount(&index))
	}

	popped = index.popRootEntry()
	if popped != first {
		t.Fatalf("second popped = %q, want first", popped.key)
	}
	if index.buckets[1] != nil {
		t.Fatal("empty root bucket remains active")
	}
	if index.bucketHeap[0].id != 3 {
		t.Fatalf("new root bucket id = %d, want 3", index.bucketHeap[0].id)
	}
}

func TestExpirationIndexResetAndSpareReuse(t *testing.T) {
	index := newExpirationIndex[string, int](time.Nanosecond)
	items := make([]*entry[string, int], maxSpareExpirationBuckets+3)
	buckets := make(map[*expirationBucket[string, int]]struct{})

	for i := range items {
		items[i] = &entry[string, int]{key: string(rune('a' + i))}
		index.update(items[i], int64(i+1))
		buckets[index.buckets[int64(i+1)]] = struct{}{}
	}
	if len(index.bucketHeap) != len(items) {
		t.Fatalf("queue len = %d, want %d", len(index.bucketHeap), len(items))
	}

	index.reset()
	if index.buckets != nil || index.bucketHeap != nil {
		t.Fatal("reset did not clear active index")
	}
	if index.spareCount != maxSpareExpirationBuckets {
		t.Fatalf("spareCount = %d, want %d", index.spareCount, maxSpareExpirationBuckets)
	}

	before := index.spareCount
	newItem := &entry[string, int]{key: "new"}
	index.update(newItem, 100)
	if index.spareCount != before-1 {
		t.Fatalf("spareCount = %d, want %d after reuse", index.spareCount, before-1)
	}
	if _, ok := buckets[index.buckets[100]]; !ok {
		t.Fatal("new bucket was not reused from spare pool")
	}
}

func TestExpirationIndexInvariantPanics(t *testing.T) {
	t.Run("missing bucket", func(t *testing.T) {
		index := newExpirationIndex[string, int](time.Nanosecond)
		item := &entry[string, int]{deadline: 1}
		requirePanic(t, func() { index.remove(item) })
	})

	t.Run("bucket underflow", func(t *testing.T) {
		bucket := &expirationBucket[string, int]{}
		requirePanic(t, func() { bucket.remove(&entry[string, int]{}) })
	})

	t.Run("empty active bucket", func(t *testing.T) {
		index := newExpirationIndex[string, int](time.Nanosecond)
		bucket := &expirationBucket[string, int]{id: 1, heapIndex: 0}
		index.bucketHeap = expirationBucketHeap[string, int]{bucket}
		index.buckets = map[int64]*expirationBucket[string, int]{1: bucket}
		requirePanic(t, func() { index.popRootEntry() })
	})

	t.Run("invalid heap value", func(t *testing.T) {
		queue := expirationBucketHeap[string, int]{}
		requirePanic(t, func() { heap.Push(&queue, "not-a-bucket") })
	})
}

func TestExpirationBucketLinkedListOperations(t *testing.T) {
	bucket := &expirationBucket[string, int]{}
	first := &entry[string, int]{key: "first"}
	second := &entry[string, int]{key: "second"}
	third := &entry[string, int]{key: "third"}

	bucket.pushBack(first)
	bucket.pushBack(second)
	bucket.pushBack(third)
	if bucket.head != first || bucket.tail != third || bucket.count != 3 {
		t.Fatal("pushBack produced invalid bucket list")
	}
	if first.expirationNext != second || second.expirationPrevious != first || second.expirationNext != third || third.expirationPrevious != second {
		t.Fatal("intrusive expiration links are inconsistent")
	}

	bucket.remove(second)
	if bucket.count != 2 || first.expirationNext != third || third.expirationPrevious != first || second.expirationPrevious != nil || second.expirationNext != nil {
		t.Fatal("remove middle item produced invalid links")
	}

	if popped := bucket.popBack(); popped != third {
		t.Fatalf("popBack() = %v, want third", popped)
	}
	if popped := bucket.popBack(); popped != first {
		t.Fatalf("popBack() = %v, want first", popped)
	}
	if popped := bucket.popBack(); popped != nil {
		t.Fatalf("popBack() on empty bucket = %v, want nil", popped)
	}
}

func TestExpirationIndexRejectsDetachedSingletonRekey(t *testing.T) {
	index := newExpirationIndex[string, int](time.Nanosecond)
	item := &entry[string, int]{key: "key", deadline: 1}
	bucket := &expirationBucket[string, int]{
		id:        1,
		heapIndex: -1,
		count:     1,
		head:      item,
		tail:      item,
	}
	index.buckets = map[int64]*expirationBucket[string, int]{1: bucket}

	if index.rekeySingletonBucket(item, 1, 2, 2) {
		t.Fatal("detached singleton bucket was rekeyed")
	}
}

func expirationIndexEntryCount[K comparable, V any](index *expirationIndex[K, V]) int {
	if index == nil {
		return 0
	}

	count := 0
	for _, bucket := range index.bucketHeap {
		count += bucket.count
	}

	return count
}
