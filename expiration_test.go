package pacecache

import (
	"container/heap"
	"fmt"
	"testing"
	"time"
)

func TestExpirationIndexBucketBoundaries(t *testing.T) {
	index := newExpirationIndex[string, int](10 * time.Nanosecond)

	bucketTests := []struct {
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

	for _, test := range bucketTests {
		if got := index.bucketID(test.deadline); got != test.want {
			t.Errorf("bucketID(%d) = %d, want %d", test.deadline, got, test.want)
		}
	}

	dueTests := []struct {
		now  int64
		want int64
	}{
		{now: -1, want: -1},
		{now: 0, want: 0},
		{now: 9, want: 0},
		{now: 10, want: 1},
		{now: 19, want: 1},
		{now: 20, want: 2},
	}

	for _, test := range dueTests {
		if got := index.dueBucketID(test.now); got != test.want {
			t.Errorf("dueBucketID(%d) = %d, want %d", test.now, got, test.want)
		}
	}

	if index.hasDueBucket(100) {
		t.Fatal("empty index reports a due bucket")
	}
}

func TestExpirationIndexDisabled(t *testing.T) {
	var index expirationIndex[string, int]
	if index.enabled() {
		t.Fatal("zero expiration index is enabled")
	}

	item := &entry[string, int]{key: "key"}
	index.update(item, 10)
	if item.deadline != 10 {
		t.Fatalf("deadline = %d, want 10", item.deadline)
	}

	index.add(item)
	index.remove(item)
	index.reset()

	if len(index.bucketHeap) != 0 || len(index.buckets) != 0 {
		t.Fatal("disabled expiration index created storage")
	}

	var nilIndex *expirationIndex[string, int]
	if nilIndex.enabled() {
		t.Fatal("nil expiration index is enabled")
	}

	other := &entry[string, int]{key: "other"}
	nilIndex.update(other, 20)
	if other.deadline != 20 {
		t.Fatalf("nil index update deadline = %d, want 20", other.deadline)
	}
}

func TestExpirationIndexUpdateTransitions(t *testing.T) {
	index := newExpirationIndex[string, int](10 * time.Nanosecond)
	item := &entry[string, int]{key: "key"}

	index.update(item, 7)
	if item.deadline != 7 {
		t.Fatalf("deadline after add = %d, want 7", item.deadline)
	}
	if len(index.bucketHeap) != 1 || len(index.buckets) != 1 {
		t.Fatalf("active buckets = (%d heap, %d map), want 1", len(index.bucketHeap), len(index.buckets))
	}

	bucket := index.buckets[1]
	if bucket == nil || bucket.count != 1 || !bucket.containsOnly(item) {
		t.Fatal("item was not added as the sole bucket entry")
	}

	index.update(item, 7)
	if index.buckets[1] != bucket || bucket.count != 1 {
		t.Fatal("same deadline changed bucket membership")
	}

	index.update(item, 9)
	if item.deadline != 9 {
		t.Fatalf("same-bucket deadline = %d, want 9", item.deadline)
	}
	if index.buckets[1] != bucket || bucket.count != 1 {
		t.Fatal("same-bucket update replaced the bucket")
	}

	index.update(item, 0)
	if item.deadline != 0 {
		t.Fatalf("deadline after NoExpiration = %d, want 0", item.deadline)
	}
	if len(index.bucketHeap) != 0 || len(index.buckets) != 0 {
		t.Fatal("NoExpiration transition left active membership")
	}
	if index.spareCount != 1 {
		t.Fatalf("spareCount = %d, want 1", index.spareCount)
	}

	index.update(item, 15)
	if item.deadline != 15 {
		t.Fatalf("deadline after re-add = %d, want 15", item.deadline)
	}
	if index.buckets[2] != bucket {
		t.Fatal("released bucket was not reused")
	}
	if index.spareCount != 0 {
		t.Fatalf("spareCount = %d, want 0", index.spareCount)
	}
}

func TestExpirationIndexMovesBetweenBuckets(t *testing.T) {
	t.Run("rekey singleton", func(t *testing.T) {
		index := newExpirationIndex[string, int](10 * time.Nanosecond)
		item := &entry[string, int]{key: "key"}

		index.update(item, 5)
		bucket := index.buckets[1]

		index.update(item, 25)

		if len(index.bucketHeap) != 1 || len(index.buckets) != 1 {
			t.Fatalf("active buckets = (%d heap, %d map), want 1", len(index.bucketHeap), len(index.buckets))
		}
		if index.buckets[3] != bucket {
			t.Fatal("singleton bucket was not rekeyed in place")
		}
		if index.buckets[1] != nil {
			t.Fatal("old bucket id remains registered")
		}
		if bucket.id != 3 || bucket.heapIndex != 0 || item.deadline != 25 {
			t.Fatalf("rekeyed state = (id=%d heap=%d deadline=%d)", bucket.id, bucket.heapIndex, item.deadline)
		}
	})

	t.Run("move into existing bucket", func(t *testing.T) {
		index := newExpirationIndex[string, int](10 * time.Nanosecond)
		first := &entry[string, int]{key: "first"}
		second := &entry[string, int]{key: "second"}

		index.update(first, 5)
		index.update(second, 25)
		destination := index.buckets[3]

		index.update(first, 24)

		if len(index.bucketHeap) != 1 || len(index.buckets) != 1 {
			t.Fatalf("active buckets = (%d heap, %d map), want 1", len(index.bucketHeap), len(index.buckets))
		}
		if index.buckets[3] != destination || destination.count != 2 {
			t.Fatal("entry was not moved into the existing destination bucket")
		}
		if first.deadline != 24 {
			t.Fatalf("first deadline = %d, want 24", first.deadline)
		}
		if index.spareCount != 1 {
			t.Fatalf("spareCount = %d, want 1", index.spareCount)
		}
	})
}

func TestExpirationIndexPopRootEntry(t *testing.T) {
	index := newExpirationIndex[string, int](10 * time.Nanosecond)
	first := &entry[string, int]{key: "first"}
	second := &entry[string, int]{key: "second"}
	third := &entry[string, int]{key: "third"}

	index.update(first, 5)
	index.update(second, 8)
	index.update(third, 15)

	if got := index.entryCount(); got != 3 {
		t.Fatalf("entryCount() = %d, want 3", got)
	}
	if index.hasDueBucket(0) {
		t.Fatal("bucket is due before its boundary")
	}
	if !index.hasDueBucket(1) {
		t.Fatal("first bucket is not due at its boundary")
	}

	if got := index.popRootEntry(); got != second {
		t.Fatalf("first pop = %q, want second", got.key)
	}
	if got := index.popRootEntry(); got != first {
		t.Fatalf("second pop = %q, want first", got.key)
	}
	if len(index.bucketHeap) != 1 || index.bucketHeap[0].id != 2 {
		t.Fatal("empty root bucket was not removed from the heap")
	}
	if got := index.popRootEntry(); got != third {
		t.Fatalf("third pop = %q, want third", got.key)
	}
	if got := index.entryCount(); got != 0 {
		t.Fatalf("entryCount() = %d, want 0", got)
	}
	if len(index.bucketHeap) != 0 || len(index.buckets) != 0 {
		t.Fatal("index is not empty after popping all entries")
	}
}

func TestExpirationIndexResetRetainsBoundedSpareBuckets(t *testing.T) {
	index := newExpirationIndex[int, int](time.Nanosecond)

	for key := 1; key <= maxSpareExpirationBuckets+2; key++ {
		index.update(&entry[int, int]{key: key}, int64(key))
	}

	index.reset()

	if index.buckets != nil {
		t.Fatalf("buckets = %v, want nil", index.buckets)
	}
	if index.bucketHeap != nil {
		t.Fatalf("bucketHeap = %v, want nil", index.bucketHeap)
	}
	if index.spareCount != maxSpareExpirationBuckets {
		t.Fatalf("spareCount = %d, want %d", index.spareCount, maxSpareExpirationBuckets)
	}

	for position := 0; position < index.spareCount; position++ {
		bucket := index.spareBuckets[position]
		if bucket == nil {
			t.Fatalf("spare bucket %d = nil", position)
		}
		if bucket.id != 0 || bucket.heapIndex != -1 || bucket.count != 0 || bucket.head != nil || bucket.tail != nil {
			t.Fatalf("spare bucket %d was not reset: %+v", position, bucket)
		}
	}
}

func TestExpirationBucketLinkedList(t *testing.T) {
	bucket := &expirationBucket[string, int]{heapIndex: -1}
	first := &entry[string, int]{key: "first"}
	second := &entry[string, int]{key: "second"}
	third := &entry[string, int]{key: "third"}

	bucket.pushBack(first)
	if !bucket.containsOnly(first) {
		t.Fatal("containsOnly(first) = false for singleton bucket")
	}

	bucket.pushBack(second)
	bucket.pushBack(third)

	if bucket.count != 3 || bucket.head != first || bucket.tail != third {
		t.Fatalf("bucket state = (count=%d head=%v tail=%v)", bucket.count, bucket.head, bucket.tail)
	}
	if first.expirationNext != second || second.expirationPrevious != first || second.expirationNext != third || third.expirationPrevious != second {
		t.Fatal("bucket links are inconsistent after pushBack")
	}

	bucket.remove(second)
	if bucket.count != 2 || first.expirationNext != third || third.expirationPrevious != first {
		t.Fatal("bucket links are inconsistent after middle removal")
	}
	if second.expirationPrevious != nil || second.expirationNext != nil {
		t.Fatal("removed entry retains expiration links")
	}

	if got := bucket.popBack(); got != third {
		t.Fatalf("popBack() = %v, want third", got)
	}
	if !bucket.containsOnly(first) {
		t.Fatal("containsOnly(first) = false after pop")
	}
	if got := bucket.popBack(); got != first {
		t.Fatalf("popBack() = %v, want first", got)
	}
	if got := bucket.popBack(); got != nil {
		t.Fatalf("popBack() = %v, want nil", got)
	}
}

func TestExpirationIndexInvariantPanics(t *testing.T) {
	requirePanic := func(t *testing.T, want string, fn func()) {
		t.Helper()

		defer func() {
			value := recover()
			if value == nil {
				t.Fatal("panic = nil")
			}

			if got := fmt.Sprint(value); got != want {
				t.Fatalf("panic = %q, want %q", got, want)
			}
		}()

		fn()
	}

	tests := []struct {
		name string
		want string
		fn   func()
	}{
		{
			name: "missing bucket",
			want: "pacecache: expiration bucket is missing",
			fn: func() {
				index := newExpirationIndex[string, int](time.Nanosecond)
				index.remove(&entry[string, int]{key: "key", deadline: 1})
			},
		},
		{
			name: "active empty bucket",
			want: "pacecache: active expiration bucket is empty",
			fn: func() {
				index := newExpirationIndex[string, int](time.Nanosecond)
				bucket := &expirationBucket[string, int]{id: 1, heapIndex: 0}
				index.buckets = map[int64]*expirationBucket[string, int]{1: bucket}
				index.bucketHeap = expirationBucketHeap[string, int]{bucket}
				index.popRootEntry()
			},
		},
		{
			name: "bucket underflow",
			want: "pacecache: expiration bucket count underflow",
			fn: func() {
				bucket := &expirationBucket[string, int]{}
				bucket.remove(&entry[string, int]{})
			},
		},
		{
			name: "invalid heap value",
			want: "pacecache: invalid expiration bucket heap value",
			fn: func() {
				queue := expirationBucketHeap[string, int]{}
				heap.Push(&queue, "invalid")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requirePanic(t, test.want, test.fn)
		})
	}
}
