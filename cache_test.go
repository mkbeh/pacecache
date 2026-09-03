package pacecache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNilCacheIsSafe(t *testing.T) {
	var cache *Cache[string, int]

	if got := cache.Name(); got != "" {
		t.Fatalf("nil Cache.Name() = %q, want empty", got)
	}
	if cache.Exists("key") {
		t.Fatal("nil Cache.Exists() = true, want false")
	}
	if cache.RefreshTTL("key") {
		t.Fatal("nil Cache.RefreshTTL() = true, want false")
	}
	if value, found := cache.Get("key"); value != 0 || found {
		t.Fatalf("nil Cache.Get() = (%d, %t), want (0, false)", value, found)
	}
	if entry, found := cache.GetEntry("key"); entry != (Entry[int]{}) || found {
		t.Fatalf("nil Cache.GetEntry() = (%+v, %t), want zero/false", entry, found)
	}
	if _, _, err := cache.GetOrLoad(context.Background(), "key"); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("nil Cache.GetOrLoad() error = %v, want ErrNotInitialized", err)
	}
	if _, _, err := cache.GetOrLoadEntry(context.Background(), "key"); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("nil Cache.GetOrLoadEntry() error = %v, want ErrNotInitialized", err)
	}

	cache.Close()
}

func TestZeroValueCacheIsSafe(t *testing.T) {
	var cache Cache[string, int]

	if cache.Exists("key") || cache.RefreshTTL("key") {
		t.Fatal("zero Cache unexpectedly reports a live key")
	}
	if value, found := cache.Get("key"); value != 0 || found {
		t.Fatalf("zero Cache.Get() = (%d, %t), want (0, false)", value, found)
	}
	if entry, found := cache.GetEntry("key"); entry != (Entry[int]{}) || found {
		t.Fatalf("zero Cache.GetEntry() = (%+v, %t), want zero/false", entry, found)
	}
	if removed := cache.CleanupExpired(); removed != 0 {
		t.Fatalf("zero Cache.CleanupExpired() = %d, want 0", removed)
	}

	cache.Set("key", 1, DefaultExpiration)
	cache.Delete()
	cache.Delete("key")
	cache.Clear()
	cache.Close()

	if got := cache.Stats(); got != (Stats{}) {
		t.Fatalf("zero Cache.Stats() = %+v, want zero Stats", got)
	}
}

func TestZeroValueCacheLoadReturnsNotInitialized(t *testing.T) {
	var cache Cache[string, int]

	_, _, err := cache.GetOrLoad(
		context.Background(),
		"key",
	)
	if !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("zero Cache.GetOrLoad() error = %v, want ErrNotInitialized", err)
	}

	_, _, err = cache.GetOrLoadEntry(
		context.Background(),
		"key",
	)
	if !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("zero Cache.GetOrLoadEntry() error = %v, want ErrNotInitialized", err)
	}
}

func TestCloseIsIdempotentAndCacheRemainsUsable(t *testing.T) {
	cache, err := New[string, int]("users")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cache.Close()
	cache.Close()
	cache.Set("key", 1, NoExpiration)
	if value, found := cache.Get("key"); value != 1 || !found {
		t.Fatalf("Get after Close = (%d, %t), want usable cache", value, found)
	}
}

func TestEffectiveTTLAndDeadlineHelpers(t *testing.T) {
	cache := &Cache[string, int]{ttl: 10 * time.Second}

	if got := cache.effectiveTTL(DefaultExpiration); got != 10*time.Second {
		t.Fatalf("default effective TTL = %v, want 10s", got)
	}
	if got := cache.effectiveTTL(3 * time.Second); got != 3*time.Second {
		t.Fatalf("custom effective TTL = %v, want 3s", got)
	}
	if got := cache.effectiveTTL(NoExpiration); got != 0 {
		t.Fatalf("NoExpiration effective TTL = %v, want 0", got)
	}

	cache.jitter = time.Second
	for range 100 {
		got := cache.effectiveTTL(5 * time.Second)
		if got < 5*time.Second || got >= 6*time.Second {
			t.Fatalf("jittered TTL = %v, want [5s,6s)", got)
		}
	}

	if got := deadlineAfter(100, 0); got != 0 {
		t.Fatalf("deadlineAfter zero = %d, want 0", got)
	}
	if got := deadlineAfter(100, -1); got != 0 {
		t.Fatalf("deadlineAfter negative = %d, want 0", got)
	}
	if got := deadlineAfter(100, 50*time.Nanosecond); got != 150 {
		t.Fatalf("deadlineAfter = %d, want 150", got)
	}
	if got := deadlineAfter(int64(maxDuration)-5, 10*time.Nanosecond); got != int64(maxDuration) {
		t.Fatalf("saturated deadline = %d, want %d", got, int64(maxDuration))
	}

	if got := jitteredTTL(5*time.Second, 0); got != 5*time.Second {
		t.Fatalf("jitteredTTL without jitter = %v", got)
	}
	if got := jitteredTTL(maxDuration, time.Second); got != maxDuration {
		t.Fatalf("jitteredTTL at max duration = %v, want maxDuration", got)
	}
}

func TestNewEnablesSlidingExpiration(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithSlidingExpiration())
	for index := range cache.store.segments {
		if !cache.store.segments[index].slidingExpiration {
			t.Fatalf("segment %d sliding expiration disabled", index)
		}
	}
}

func TestNewWrapsConfigurationError(t *testing.T) {
	cache, err := New[string, int]("")
	if cache != nil {
		t.Fatal("cache must be nil for invalid configuration")
	}
	if err == nil || err.Error() != "pacecache: invalid configuration: cache name must not be empty" {
		t.Fatalf("New() error = %v", err)
	}
}

type testCompositeKey struct {
	TenantID int64
	UserID   int64
}

func TestCacheSupportsInt64Keys(t *testing.T) {
	cache, err := New[int64, string]("users", WithMaxEntries(8), WithSegmentCount(2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(cache.Close)

	cache.Set(42, "Ada", NoExpiration)

	if !cache.Exists(42) || !cache.RefreshTTL(42) {
		t.Fatal("int64 key was not found")
	}
	if value, found := cache.Get(42); value != "Ada" || !found {
		t.Fatalf("Get(42) = (%q, %t), want (Ada, true)", value, found)
	}

	cache.Delete(42)
	if cache.Exists(42) || cache.RefreshTTL(42) {
		t.Fatal("int64 key remains after Delete")
	}
	if _, found := cache.Get(42); found {
		t.Fatal("Get(42) hit after Delete")
	}
}

func TestCacheSupportsComparableStructKeys(t *testing.T) {
	cache, err := New[testCompositeKey, int]("users", WithMaxEntries(8), WithSegmentCount(2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(cache.Close)

	stored := testCompositeKey{TenantID: 7, UserID: 42}
	equal := testCompositeKey{TenantID: 7, UserID: 42}

	cache.Set(stored, 100, NoExpiration)

	if !cache.Exists(equal) || !cache.RefreshTTL(equal) {
		t.Fatal("equal comparable struct key was not found")
	}
	if value, found := cache.Get(equal); value != 100 || !found {
		t.Fatalf("Get(equal) = (%d, %t), want (100, true)", value, found)
	}

	cache.Delete(equal)
	if _, found := cache.Get(stored); found {
		t.Fatal("equal-key Delete did not remove stored key")
	}
}

type observedWaitContext struct {
	context.Context

	done       chan struct{}
	waiting    chan struct{}
	waitOnce   sync.Once
	cancelOnce sync.Once
}

func newObservedWaitContext() *observedWaitContext {
	return &observedWaitContext{
		Context: context.Background(),
		done:    make(chan struct{}),
		waiting: make(chan struct{}),
	}
}

func (ctx *observedWaitContext) Done() <-chan struct{} {
	ctx.waitOnce.Do(func() {
		close(ctx.waiting)
	})

	return ctx.done
}

func (ctx *observedWaitContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func (ctx *observedWaitContext) cancel() {
	ctx.cancelOnce.Do(func() {
		close(ctx.done)
	})
}

const testTimeout = 5 * time.Second

func mustNewCache[V any](t *testing.T, name string, options ...Option) *Cache[string, V] {
	t.Helper()

	cache, err := New[string, V](name, options...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Cleanup(cache.Close)

	return cache
}

func requirePanic(t *testing.T, fn func()) {
	t.Helper()

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()

		fn()
	}()

	if recovered == nil {
		t.Fatal("expected panic")
	}
}

func receiveTestValue[T any](t *testing.T, channel <-chan T) T {
	t.Helper()

	select {
	case value := <-channel:
		return value
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for value")
	}

	var zero T
	return zero
}

func waitTestSignal(t *testing.T, channel <-chan struct{}) {
	t.Helper()

	select {
	case <-channel:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for signal")
	}
}

func waitTestGroup(t *testing.T, group *sync.WaitGroup) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()

	waitTestSignal(t, done)
}
