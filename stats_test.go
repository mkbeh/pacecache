package pacecache

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestStatsZeroValueAndNilCache(t *testing.T) {
	var nilCache *Cache[string, int]
	if got := nilCache.Stats(); got != (Stats{}) {
		t.Fatalf("nil Cache Stats() = %+v, want zero Stats", got)
	}

	var zeroCache Cache[string, int]
	if got := zeroCache.Stats(); got != (Stats{}) {
		t.Fatalf("zero Cache Stats() = %+v, want zero Stats", got)
	}
}

func TestStatsAggregatesSegments(t *testing.T) {
	store := newStorageWithSegments[string, int](8, 2)
	stats := newStatsCollector(2)
	cache := &Cache[string, int]{
		store:  store,
		states: newCacheStates[string, int](2),
		stats:  stats,
	}

	store.segments[0].set("first", cachedValue[int]{value: 1, found: true}, 0, stats.segment(0))
	store.segments[1].set("second", cachedValue[int]{value: 2, found: true}, 0, stats.segment(1))
	store.segments[1].set("third", cachedValue[int]{found: false}, 0, stats.segment(1))

	stats.segments[0].hitCount = 2
	stats.segments[0].negativeHitCount = 3
	stats.segments[0].missCount = 4
	stats.segments[0].evictionCount = 5
	stats.segments[0].expirationCount = 6
	stats.segments[0].loadFoundCount.Store(7)
	stats.segments[0].loadNotFoundCount.Store(8)
	stats.segments[0].loadErrorCount.Store(9)
	stats.segments[0].loadSupersededCount.Store(10)
	stats.segments[0].loadDurationNanos.Store(int64(11 * time.Millisecond))
	stats.segments[0].sharedCount.Store(12)
	stats.segments[0].invalidatedKeyCount.Store(13)

	stats.segments[1].hitCount = 20
	stats.segments[1].negativeHitCount = 30
	stats.segments[1].missCount = 40
	stats.segments[1].evictionCount = 50
	stats.segments[1].expirationCount = 60
	stats.segments[1].loadFoundCount.Store(70)
	stats.segments[1].loadNotFoundCount.Store(80)
	stats.segments[1].loadErrorCount.Store(90)
	stats.segments[1].loadSupersededCount.Store(100)
	stats.segments[1].loadDurationNanos.Store(int64(110 * time.Millisecond))
	stats.segments[1].sharedCount.Store(120)
	stats.segments[1].invalidatedKeyCount.Store(130)
	stats.invalidatedAllCount.Store(140)

	got := cache.Stats()
	want := Stats{
		EntryCount:          3,
		MaxEntries:          8,
		SegmentCount:        2,
		HitCount:            22,
		NegativeHitCount:    33,
		MissCount:           44,
		LoadFoundCount:      77,
		LoadNotFoundCount:   88,
		LoadErrorCount:      99,
		LoadSupersededCount: 110,
		LoadDuration:        121 * time.Millisecond,
		SharedCount:         132,
		InvalidatedKeyCount: 143,
		InvalidatedAllCount: 140,
		EvictionCount:       55,
		ExpirationCount:     66,
	}

	if got != want {
		t.Fatalf("Stats() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestStatsRecordLoad(t *testing.T) {
	stats := newStatsCollector(1)
	sentinel := errors.New("load failed")

	stats.recordLoad(0, true, nil, 3*time.Millisecond)
	stats.recordLoad(0, false, nil, 5*time.Millisecond)
	stats.recordLoad(0, true, sentinel, 7*time.Millisecond)

	segment := stats.segment(0)
	if got := segment.loadFoundCount.Load(); got != 1 {
		t.Fatalf("loadFoundCount = %d, want 1", got)
	}
	if got := segment.loadNotFoundCount.Load(); got != 1 {
		t.Fatalf("loadNotFoundCount = %d, want 1", got)
	}
	if got := segment.loadErrorCount.Load(); got != 1 {
		t.Fatalf("loadErrorCount = %d, want 1", got)
	}
	if got := time.Duration(segment.loadDurationNanos.Load()); got != 15*time.Millisecond {
		t.Fatalf("loadDuration = %v, want %v", got, 15*time.Millisecond)
	}
}

func TestStatsRecordHelpers(t *testing.T) {
	stats := newStatsCollector(2)

	stats.recordLoadSuperseded(0)
	stats.recordShared(0)
	stats.recordKeyInvalidation(0, 3)
	stats.recordKeyInvalidation(0, 0)
	stats.recordKeyInvalidation(0, -1)
	stats.recordAllInvalidation(4)
	stats.recordAllInvalidation(0)
	stats.recordAllInvalidation(-1)

	if got := stats.segment(0).loadSupersededCount.Load(); got != 1 {
		t.Fatalf("loadSupersededCount = %d, want 1", got)
	}
	if got := stats.segment(0).sharedCount.Load(); got != 1 {
		t.Fatalf("sharedCount = %d, want 1", got)
	}
	if got := stats.segment(0).invalidatedKeyCount.Load(); got != 3 {
		t.Fatalf("invalidatedKeyCount = %d, want 3", got)
	}
	if got := stats.invalidatedAllCount.Load(); got != 4 {
		t.Fatalf("invalidatedAllCount = %d, want 4", got)
	}
}

func TestStatsConcurrentWithCacheOperations(t *testing.T) {
	cache, err := New[int, int](
		"concurrent",
		WithMaxEntries(128),
		WithSegmentCount(8),
		WithTTL(NoExpiration),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		for index := 0; index < iterations; index++ {
			cache.Set(index%64, index, NoExpiration)
		}
	}()

	go func() {
		defer wg.Done()
		for index := 0; index < iterations; index++ {
			cache.Get(index % 64)
		}
	}()

	go func() {
		defer wg.Done()
		for index := 0; index < iterations; index++ {
			cache.Invalidate(index % 64)
		}
	}()

	go func() {
		defer wg.Done()
		for range iterations {
			_ = cache.Stats()
		}
	}()

	wg.Wait()

	stats := cache.Stats()
	if stats.MaxEntries != 128 || stats.SegmentCount != 8 {
		t.Fatalf("Stats() configuration = (%d, %d), want (128, 8)", stats.MaxEntries, stats.SegmentCount)
	}
}
