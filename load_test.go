package pacecache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetOrLoadCachesPositiveResult(t *testing.T) {
	cache := mustNewCache[int](t, "users")
	var calls atomic.Int64

	loader := func(context.Context) (int, bool, error) {
		calls.Add(1)
		return 42, true, nil
	}

	for range 2 {
		value, found, err := cache.GetOrLoad(context.Background(), "key", loader)
		if err != nil || !found || value != 42 {
			t.Fatalf("GetOrLoad() = (%d, %t, %v), want (42, true, nil)", value, found, err)
		}
	}

	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", calls.Load())
	}
	if value, found := cache.Get("key"); value != 42 || !found {
		t.Fatalf("Get(key) = (%d, %t), want cached hit", value, found)
	}
}

func TestGetOrLoadNotFoundIsNotCached(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(8), WithSegmentCount(1))
	var calls atomic.Int64

	loader := func(context.Context) (int, bool, error) {
		calls.Add(1)
		return 99, false, nil
	}

	for range 2 {
		value, found, err := cache.GetOrLoad(context.Background(), "key", loader)
		if err != nil || found || value != 0 {
			t.Fatalf("GetOrLoad() = (%d, %t, %v), want (0, false, nil)", value, found, err)
		}
		if cache.Exists("key") {
			t.Fatal("found=false result became resident")
		}
	}

	if calls.Load() != 2 {
		t.Fatalf("loader calls = %d, want 2", calls.Load())
	}
	stats := cache.Stats()
	if stats.LoadNotFoundCount != 2 || stats.LoadFoundCount != 0 || stats.LoadErrorCount != 0 {
		t.Fatalf("load stats = %+v, want not-found=2", stats)
	}
}

func TestGetOrLoadEntryPositiveAndNotFound(t *testing.T) {
	cache := mustNewCache[int](
		t,
		"users",
		WithMaxEntries(8),
		WithSegmentCount(1),
		WithTTL(time.Minute),
	)

	entry, found, err := cache.GetOrLoadEntry(
		context.Background(),
		"positive",
		func(context.Context) (int, bool, error) { return 42, true, nil },
	)
	if err != nil || !found || entry.Value() != 42 || entry.ExpiresAt().IsZero() {
		t.Fatalf("GetOrLoadEntry(positive) = (%+v, %t, %v)", entry, found, err)
	}

	entry, found, err = cache.GetOrLoadEntry(
		context.Background(),
		"missing",
		func(context.Context) (int, bool, error) { return 99, false, nil },
	)
	if err != nil || found || entry != (Entry[int]{}) {
		t.Fatalf("GetOrLoadEntry(missing) = (%+v, %t, %v), want zero/false/nil", entry, found, err)
	}
	if cache.Exists("missing") {
		t.Fatal("GetOrLoadEntry found=false result became resident")
	}
}

func TestGetOrLoadErrorsAreNotCached(t *testing.T) {
	cache := mustNewCache[int](t, "users")
	sentinel := errors.New("load failed")
	var calls atomic.Int64

	loader := func(context.Context) (int, bool, error) {
		calls.Add(1)
		return 123, true, sentinel
	}

	for range 2 {
		value, found, err := cache.GetOrLoad(context.Background(), "key", loader)
		if value != 0 || found || !errors.Is(err, sentinel) {
			t.Fatalf("GetOrLoad() = (%d, %t, %v), want zero/false/sentinel", value, found, err)
		}
	}

	if calls.Load() != 2 {
		t.Fatalf("loader calls = %d, want 2", calls.Load())
	}
	if cache.Exists("key") {
		t.Fatal("loader error was cached")
	}
}

func TestGetOrLoadLoaderPanicPropagatesToCallerAndDoesNotPoisonKey(t *testing.T) {
	cache := mustNewCache[int](t, "users")
	wantErr := errors.New("boom")

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()

		_, _, _ = cache.GetOrLoad(
			context.Background(),
			"key",
			func(context.Context) (int, bool, error) {
				panic(wantErr)
			},
		)
	}()

	panicErr, ok := recovered.(error)
	if !ok {
		t.Fatalf("panic = %T(%v), want error", recovered, recovered)
	}
	if !errors.Is(panicErr, wantErr) {
		t.Fatalf("panic error = %v, want errors.Is(..., %v)", panicErr, wantErr)
	}
	if cache.Exists("key") {
		t.Fatal("panicking loader result became resident")
	}

	value, found, err := cache.GetOrLoad(
		context.Background(),
		"key",
		func(context.Context) (int, bool, error) {
			return 42, true, nil
		},
	)
	if err != nil || !found || value != 42 {
		t.Fatalf("next GetOrLoad() = (%d, %t, %v), want (42, true, nil)", value, found, err)
	}
}

func TestGetOrLoadValidatesContextAndLoader(t *testing.T) {
	cache := mustNewCache[int](t, "users")

	if _, _, err := cache.GetOrLoad(nil, "key", func(context.Context) (int, bool, error) {
		return 1, true, nil
	}); err == nil || err.Error() != "pacecache: context is nil" {
		t.Fatalf("nil context error = %v", err)
	}

	if _, _, err := cache.GetOrLoad(context.Background(), "key", nil); err == nil || err.Error() != "pacecache: loader is nil" {
		t.Fatalf("nil loader error = %v", err)
	}

	if _, _, err := cache.GetOrLoadEntry(nil, "key", func(context.Context) (int, bool, error) {
		return 1, true, nil
	}); err == nil || err.Error() != "pacecache: context is nil" {
		t.Fatalf("GetOrLoadEntry nil context error = %v", err)
	}
}

func TestGetOrLoadCanceledMissDoesNotStartLoader(t *testing.T) {
	cache := mustNewCache[int](t, "users")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls atomic.Int64
	value, found, err := cache.GetOrLoad(ctx, "key", func(context.Context) (int, bool, error) {
		calls.Add(1)
		return 1, true, nil
	})
	if value != 0 || found || !errors.Is(err, context.Canceled) {
		t.Fatalf("GetOrLoad() = (%d, %t, %v), want zero/false/context.Canceled", value, found, err)
	}
	if calls.Load() != 0 {
		t.Fatalf("loader calls = %d, want 0", calls.Load())
	}
}

func TestGetOrLoadCachedHitIgnoresCanceledContext(t *testing.T) {
	cache := mustNewCache[int](t, "users")
	cache.Set("key", 7, NoExpiration)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	value, found, err := cache.GetOrLoad(ctx, "key", func(context.Context) (int, bool, error) {
		t.Fatal("loader called for cached hit")
		return 0, false, nil
	})
	if err != nil || !found || value != 7 {
		t.Fatalf("GetOrLoad() = (%d, %t, %v), want cached 7", value, found, err)
	}
}

func TestGetOrLoadCoalescesConcurrentMisses(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(32), WithSegmentCount(1))

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64

	loader := func(ctx context.Context) (int, bool, error) {
		if calls.Add(1) == 1 {
			close(started)
		}

		select {
		case <-release:
			return 42, true, nil
		case <-ctx.Done():
			return 0, false, ctx.Err()
		}
	}

	ownerDone := make(chan error, 1)
	go func() {
		value, found, err := cache.GetOrLoad(context.Background(), "key", loader)
		if err == nil && (!found || value != 42) {
			err = errors.New("unexpected owner result")
		}
		ownerDone <- err
	}()
	waitTestSignal(t, started)

	const waiters = 31
	var group sync.WaitGroup
	group.Add(waiters)
	errs := make(chan error, waiters)
	waiterContexts := make([]*observedWaitContext, waiters)

	for index := range waiters {
		waiterContext := newObservedWaitContext()
		waiterContexts[index] = waiterContext

		go func(ctx context.Context) {
			defer group.Done()

			value, found, err := cache.GetOrLoad(ctx, "key", loader)
			if err != nil {
				errs <- err
				return
			}
			if !found || value != 42 {
				errs <- errors.New("unexpected waiter result")
			}
		}(waiterContext)
	}

	for _, waiterContext := range waiterContexts {
		waitTestSignal(t, waiterContext.waiting)
	}
	close(release)

	if err := receiveTestValue(t, ownerDone); err != nil {
		t.Fatal(err)
	}
	waitTestGroup(t, &group)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
	if got := cache.Stats().SharedCount; got == 0 {
		t.Fatal("SharedCount = 0, want shared callers recorded")
	}
}

func TestGetOrLoadWaiterCanCancelWithoutCancelingSharedLoad(t *testing.T) {
	cache := mustNewCache[int](t, "users", WithMaxEntries(8), WithSegmentCount(1))

	started := make(chan struct{})
	release := make(chan struct{})
	ownerDone := make(chan error, 1)

	go func() {
		value, found, err := cache.GetOrLoad(
			context.Background(),
			"key",
			func(context.Context) (int, bool, error) {
				close(started)
				<-release
				return 5, true, nil
			},
		)
		if err == nil && (!found || value != 5) {
			err = errors.New("unexpected owner result")
		}
		ownerDone <- err
	}()
	waitTestSignal(t, started)

	waiterContext := newObservedWaitContext()
	waiterDone := make(chan error, 1)
	go func() {
		_, _, err := cache.GetOrLoad(waiterContext, "key", func(context.Context) (int, bool, error) {
			return 99, true, nil
		})
		waiterDone <- err
	}()

	waitTestSignal(t, waiterContext.waiting)
	waiterContext.cancel()
	if err := receiveTestValue(t, waiterDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error = %v, want context.Canceled", err)
	}

	close(release)
	if err := receiveTestValue(t, ownerDone); err != nil {
		t.Fatal(err)
	}
	if value, found := cache.Get("key"); value != 5 || !found {
		t.Fatalf("Get(key) = (%d, %t), want completed shared load", value, found)
	}
}

func TestGetOrLoadAndGetOrLoadEntryShareOneWave(t *testing.T) {
	cache := mustNewCache[int](
		t,
		"users",
		WithMaxEntries(8),
		WithSegmentCount(1),
		WithTTL(time.Minute),
	)

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64

	valueDone := make(chan struct {
		value int
		found bool
		err   error
	}, 1)
	go func() {
		value, found, err := cache.GetOrLoad(
			context.Background(),
			"key",
			func(context.Context) (int, bool, error) {
				calls.Add(1)
				close(started)
				<-release
				return 42, true, nil
			},
		)
		valueDone <- struct {
			value int
			found bool
			err   error
		}{value: value, found: found, err: err}
	}()

	waitTestSignal(t, started)

	entryContext := newObservedWaitContext()
	entryDone := make(chan struct {
		entry Entry[int]
		found bool
		err   error
	}, 1)
	go func() {
		entry, found, err := cache.GetOrLoadEntry(
			entryContext,
			"key",
			func(context.Context) (int, bool, error) {
				calls.Add(1)
				return 99, true, nil
			},
		)
		entryDone <- struct {
			entry Entry[int]
			found bool
			err   error
		}{entry: entry, found: found, err: err}
	}()

	waitTestSignal(t, entryContext.waiting)
	close(release)

	valueResult := receiveTestValue(t, valueDone)
	entryResult := receiveTestValue(t, entryDone)

	if valueResult.err != nil || !valueResult.found || valueResult.value != 42 {
		t.Fatalf("GetOrLoad() = (%d, %t, %v), want (42, true, nil)", valueResult.value, valueResult.found, valueResult.err)
	}
	if entryResult.err != nil || !entryResult.found || entryResult.entry.Value() != 42 || entryResult.entry.ExpiresAt().IsZero() {
		t.Fatalf("GetOrLoadEntry() = (%+v, %t, %v), want shared value 42", entryResult.entry, entryResult.found, entryResult.err)
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", calls.Load())
	}

	item := cache.store.segments[0].entries["key"]
	if item == nil {
		t.Fatal("published entry is missing")
	}
	if want := cache.store.expiresAt(item.deadline); !entryResult.entry.ExpiresAt().Equal(want) {
		t.Fatalf("Entry.ExpiresAt() = %v, want exact published deadline %v", entryResult.entry.ExpiresAt(), want)
	}
}

func TestGetOrLoadUsesGenericKeyIdentity(t *testing.T) {
	cache, err := New[testCompositeKey, int]("users", WithMaxEntries(8), WithSegmentCount(2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(cache.Close)

	key := testCompositeKey{TenantID: 7, UserID: 42}
	equal := testCompositeKey{TenantID: 7, UserID: 42}
	var calls atomic.Int64

	loader := func(context.Context) (int, bool, error) {
		calls.Add(1)
		return 100, true, nil
	}

	value, found, err := cache.GetOrLoad(context.Background(), key, loader)
	if err != nil || !found || value != 100 {
		t.Fatalf("first GetOrLoad() = (%d, %t, %v)", value, found, err)
	}
	value, found, err = cache.GetOrLoad(context.Background(), equal, loader)
	if err != nil || !found || value != 100 {
		t.Fatalf("equal-key GetOrLoad() = (%d, %t, %v)", value, found, err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
}

type publicationLoadResult[V any] struct {
	value V
	found bool
	err   error
}

func TestGetOrLoadSetSameKeySupersedesInflightFoundLoad(t *testing.T) {
	cache := newPublicationTestCache(t)
	started, release, result := startPublicationLoad(cache, "key", "old", true, nil)

	waitTestSignal(t, started)
	cache.Set("key", "new", NoExpiration)
	close(release)

	assertPublicationSuperseded(t, receiveTestValue(t, result))
	assertCachedValue(t, cache, "key", "new")

	stats := cache.Stats()
	if stats.LoadFoundCount != 1 || stats.LoadSupersededCount != 1 || stats.LoadErrorCount != 0 {
		t.Fatalf("load stats = %+v, want found=1 superseded=1 errors=0", stats)
	}
}

func TestGetOrLoadInvalidateSameKeySupersedesInflightFoundLoad(t *testing.T) {
	cache := newPublicationTestCache(t)
	started, release, result := startPublicationLoad(cache, "key", "old", true, nil)

	waitTestSignal(t, started)
	cache.Invalidate("key")
	close(release)

	assertPublicationSuperseded(t, receiveTestValue(t, result))
	assertCacheMiss(t, cache, "key")

	stats := cache.Stats()
	if stats.LoadFoundCount != 1 || stats.LoadSupersededCount != 1 {
		t.Fatalf("load stats = %+v, want found=1 superseded=1", stats)
	}
	if stats.InvalidatedKeyCount != 0 {
		t.Fatalf("InvalidatedKeyCount = %d, want 0 for missing resident key", stats.InvalidatedKeyCount)
	}
}

func TestGetOrLoadSetSameKeySupersedesInflightNotFoundLoad(t *testing.T) {
	cache := newPublicationTestCache(t)
	started, release, result := startPublicationLoad(cache, "key", "ignored", false, nil)

	waitTestSignal(t, started)
	cache.Set("key", "new", NoExpiration)
	close(release)

	assertPublicationSuperseded(t, receiveTestValue(t, result))
	assertCachedValue(t, cache, "key", "new")

	stats := cache.Stats()
	if stats.LoadNotFoundCount != 1 || stats.LoadSupersededCount != 1 || stats.LoadErrorCount != 0 {
		t.Fatalf("load stats = %+v, want not-found=1 superseded=1 errors=0", stats)
	}
}

func TestGetOrLoadInvalidateSameKeySupersedesInflightNotFoundLoad(t *testing.T) {
	cache := newPublicationTestCache(t)
	started, release, result := startPublicationLoad(cache, "key", "ignored", false, nil)

	waitTestSignal(t, started)
	cache.Invalidate("key")
	close(release)

	assertPublicationSuperseded(t, receiveTestValue(t, result))
	assertCacheMiss(t, cache, "key")

	stats := cache.Stats()
	if stats.LoadNotFoundCount != 1 || stats.LoadSupersededCount != 1 || stats.LoadErrorCount != 0 {
		t.Fatalf("load stats = %+v, want not-found=1 superseded=1 errors=0", stats)
	}
}

func TestGetOrLoadLoaderErrorPrecedesSuperseded(t *testing.T) {
	cache := newPublicationTestCache(t)
	sentinel := errors.New("loader failed")
	started, release, result := startPublicationLoad(cache, "key", "ignored", false, sentinel)

	waitTestSignal(t, started)
	cache.Invalidate("key")
	close(release)

	loaded := receiveTestValue(t, result)
	if loaded.value != "" || loaded.found || !errors.Is(loaded.err, sentinel) || errors.Is(loaded.err, ErrLoadSuperseded) {
		t.Fatalf("GetOrLoad() = (%q, %t, %v), want zero/false/loader error", loaded.value, loaded.found, loaded.err)
	}

	stats := cache.Stats()
	if stats.LoadErrorCount != 1 || stats.LoadSupersededCount != 0 || stats.LoadNotFoundCount != 0 {
		t.Fatalf("load stats = %+v, want errors=1 superseded=0", stats)
	}
}

func TestGetOrLoadSetOtherKeySameSegmentDoesNotSupersede(t *testing.T) {
	cache := newPublicationTestCache(t)
	started, release, result := startPublicationLoad(cache, "key-1", "loaded", true, nil)

	waitTestSignal(t, started)
	cache.Set("key-2", "other", NoExpiration)
	close(release)

	assertPublicationSuccess(t, receiveTestValue(t, result), "loaded")
	assertCachedValue(t, cache, "key-1", "loaded")
	assertCachedValue(t, cache, "key-2", "other")
}

func TestGetOrLoadInvalidateOtherKeySameSegmentDoesNotSupersede(t *testing.T) {
	cache := newPublicationTestCache(t)
	cache.Set("key-2", "other", NoExpiration)
	started, release, result := startPublicationLoad(cache, "key-1", "loaded", true, nil)

	waitTestSignal(t, started)
	cache.Invalidate("key-2")
	close(release)

	assertPublicationSuccess(t, receiveTestValue(t, result), "loaded")
	assertCachedValue(t, cache, "key-1", "loaded")
	assertCacheMiss(t, cache, "key-2")
}

func TestGetOrLoadBatchInvalidateOtherKeysSameSegmentDoesNotSupersede(t *testing.T) {
	cache := newPublicationTestCache(t)
	cache.Set("key-2", "other-2", NoExpiration)
	cache.Set("key-3", "other-3", NoExpiration)
	started, release, result := startPublicationLoad(cache, "key-1", "loaded", true, nil)

	waitTestSignal(t, started)
	cache.Invalidate("key-2", "key-3", "missing")
	close(release)

	assertPublicationSuccess(t, receiveTestValue(t, result), "loaded")
	assertCachedValue(t, cache, "key-1", "loaded")
}

func TestGetOrLoadInvalidateAllSupersedesAllInflightLoads(t *testing.T) {
	cache := newPublicationTestCache(t)

	firstStarted, firstRelease, firstResult := startPublicationLoad(cache, "first", "one", true, nil)
	secondStarted, secondRelease, secondResult := startPublicationLoad(cache, "second", "two", true, nil)

	waitTestSignal(t, firstStarted)
	waitTestSignal(t, secondStarted)

	cache.InvalidateAll()
	close(firstRelease)
	close(secondRelease)

	assertPublicationSuperseded(t, receiveTestValue(t, firstResult))
	assertPublicationSuperseded(t, receiveTestValue(t, secondResult))
	assertCacheMiss(t, cache, "first")
	assertCacheMiss(t, cache, "second")

	stats := cache.Stats()
	if stats.LoadFoundCount != 2 || stats.LoadSupersededCount != 2 || stats.LoadErrorCount != 0 {
		t.Fatalf("load stats = %+v, want found=2 superseded=2", stats)
	}
}

func TestGetOrLoadEntrySetSameKeyReturnsSupersededZeroEntry(t *testing.T) {
	cache := newPublicationTestCache(t)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct {
		entry Entry[string]
		found bool
		err   error
	}, 1)

	go func() {
		entry, found, err := cache.GetOrLoadEntry(
			context.Background(),
			"key",
			func(context.Context) (string, bool, error) {
				close(started)
				<-release
				return "old", true, nil
			},
		)
		done <- struct {
			entry Entry[string]
			found bool
			err   error
		}{entry: entry, found: found, err: err}
	}()

	waitTestSignal(t, started)
	cache.Set("key", "new", NoExpiration)
	close(release)

	result := receiveTestValue(t, done)
	if result.entry != (Entry[string]{}) || result.found || !errors.Is(result.err, ErrLoadSuperseded) {
		t.Fatalf("GetOrLoadEntry() = (%+v, %t, %v), want zero/false/ErrLoadSuperseded", result.entry, result.found, result.err)
	}
	assertCachedValue(t, cache, "key", "new")
}

func startPublicationLoad(
	cache *Cache[string, string],
	key string,
	value string,
	found bool,
	loaderErr error,
) (<-chan struct{}, chan struct{}, <-chan publicationLoadResult[string]) {
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan publicationLoadResult[string], 1)

	go func() {
		loaded, gotFound, err := cache.GetOrLoad(
			context.Background(),
			key,
			func(context.Context) (string, bool, error) {
				close(started)
				<-release
				return value, found, loaderErr
			},
		)

		result <- publicationLoadResult[string]{
			value: loaded,
			found: gotFound,
			err:   err,
		}
	}()

	return started, release, result
}

func assertPublicationSuperseded(t *testing.T, result publicationLoadResult[string]) {
	t.Helper()

	if result.value != "" || result.found || !errors.Is(result.err, ErrLoadSuperseded) {
		t.Fatalf("GetOrLoad() = (%q, %t, %v), want zero/false/ErrLoadSuperseded", result.value, result.found, result.err)
	}
}

func assertPublicationSuccess(t *testing.T, result publicationLoadResult[string], want string) {
	t.Helper()

	if result.err != nil || !result.found || result.value != want {
		t.Fatalf("GetOrLoad() = (%q, %t, %v), want (%q, true, nil)", result.value, result.found, result.err, want)
	}
}

func assertCachedValue(t *testing.T, cache *Cache[string, string], key, want string) {
	t.Helper()

	value, found := cache.Get(key)
	if !found || value != want {
		t.Fatalf("Get(%q) = (%q, %t), want (%q, true)", key, value, found, want)
	}
}

func assertCacheMiss(t *testing.T, cache *Cache[string, string], key string) {
	t.Helper()

	value, found := cache.Get(key)
	if found || value != "" {
		t.Fatalf("Get(%q) = (%q, %t), want zero/false", key, value, found)
	}
}

func newPublicationTestCache(t *testing.T) *Cache[string, string] {
	t.Helper()

	cache, err := New[string, string](
		"publication-test",
		WithMaxEntries(64),
		WithSegmentCount(1),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Cleanup(cache.Close)
	return cache
}
