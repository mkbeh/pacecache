package singleflight

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const singleflightTestTimeout = 5 * time.Second

func TestStartCallAndDoCallReturnValue(t *testing.T) {
	var group Group[string, int]

	call, shouldLoad := group.StartCall("key")
	if !shouldLoad {
		t.Fatal("StartCall() shouldLoad = false, want true")
	}

	group.DoCall("key", call, func(state CallState) (int, error) {
		if state.Forgotten() {
			return 0, errors.New("new call is unexpectedly forgotten")
		}

		return 42, nil
	})

	result, err := call.Wait(context.Background())
	if err != nil || result.Err != nil || result.Val != 42 || result.Shared {
		t.Fatalf("result = %+v, wait error = %v, want value=42 shared=false", result, err)
	}
}

func TestDoCallReturnsLoaderError(t *testing.T) {
	var group Group[string, int]

	wantErr := errors.New("load failed")
	call, shouldLoad := group.StartCall("key")
	if !shouldLoad {
		t.Fatal("StartCall() shouldLoad = false, want true")
	}

	group.DoCall("key", call, func(CallState) (int, error) {
		return 0, wantErr
	})

	result, waitErr := call.Wait(context.Background())
	if waitErr != nil {
		t.Fatalf("Wait() error = %v, want nil", waitErr)
	}
	if !errors.Is(result.Err, wantErr) || result.Val != 0 || result.Shared {
		t.Fatalf("result = %+v, want zero value, loader error, shared=false", result)
	}
}

func TestStartCallSuppressesConcurrentCallers(t *testing.T) {
	var group Group[string, int]

	const callers = 256

	type registration struct {
		call       Call[int]
		shouldLoad bool
	}

	start := make(chan struct{})
	registrations := make(chan registration, callers)

	var waiters sync.WaitGroup
	waiters.Add(callers)

	for range callers {
		go func() {
			defer waiters.Done()
			<-start

			call, shouldLoad := group.StartCall("key")
			registrations <- registration{
				call:       call,
				shouldLoad: shouldLoad,
			}
		}()
	}

	close(start)
	waitGroup(t, &waiters)
	close(registrations)

	var (
		owner      Call[int]
		allCalls   []Call[int]
		ownerCount int
	)

	for registered := range registrations {
		allCalls = append(allCalls, registered.call)
		if registered.shouldLoad {
			owner = registered.call
			ownerCount++
		}
	}

	if ownerCount != 1 {
		t.Fatalf("owners = %d, want 1", ownerCount)
	}

	var loaderCalls atomic.Int64
	group.DoCall("key", owner, func(CallState) (int, error) {
		loaderCalls.Add(1)
		return 42, nil
	})

	for index, call := range allCalls {
		result, err := call.Wait(context.Background())
		if err != nil || result.Err != nil || result.Val != 42 || !result.Shared {
			t.Fatalf("caller %d result = %+v, wait error = %v", index, result, err)
		}
	}

	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
}

func TestStartCallKeepsDifferentKeysIndependent(t *testing.T) {
	var group Group[int, int]

	const keys = 64

	for key := range keys {
		owner, shouldLoad := group.StartCall(key)
		if !shouldLoad {
			t.Fatalf("key %d owner shouldLoad = false", key)
		}

		duplicate, duplicateShouldLoad := group.StartCall(key)
		if duplicateShouldLoad {
			t.Fatalf("key %d duplicate shouldLoad = true", key)
		}

		group.DoCall(key, owner, func(CallState) (int, error) {
			return key, nil
		})

		for _, call := range []Call[int]{owner, duplicate} {
			result, err := call.Wait(context.Background())
			if err != nil || result.Err != nil || result.Val != key || !result.Shared {
				t.Fatalf("key %d result = %+v, wait error = %v", key, result, err)
			}
		}
	}
}

func TestWaitCancellationDoesNotCancelWave(t *testing.T) {
	var group Group[string, int]

	owner, shouldLoad := group.StartCall("key")
	if !shouldLoad {
		t.Fatal("owner shouldLoad = false")
	}
	waiter, shouldLoad := group.StartCall("key")
	if shouldLoad {
		t.Fatal("waiter shouldLoad = true")
	}

	started := make(chan struct{})
	release := make(chan struct{})
	ownerDone := make(chan struct{})

	go func() {
		defer close(ownerDone)

		group.DoCall("key", owner, func(CallState) (int, error) {
			close(started)
			<-release

			return 42, nil
		})
	}()

	waitClosed(t, started)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := waiter.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context.Canceled", err)
	}

	close(release)
	waitClosed(t, ownerDone)

	result, err := owner.Wait(context.Background())
	if err != nil || result.Err != nil || result.Val != 42 || !result.Shared {
		t.Fatalf("owner result = %+v, wait error = %v", result, err)
	}
}

func TestForgetMarksOldWaveAndStartsNewWave(t *testing.T) {
	var group Group[string, int]

	oldCall, shouldLoad := group.StartCall("key")
	if !shouldLoad {
		t.Fatal("old call shouldLoad = false")
	}

	group.Forget("key")

	newCall, shouldLoad := group.StartCall("key")
	if !shouldLoad {
		t.Fatal("new call shouldLoad = false")
	}

	newStarted := make(chan struct{})
	newRelease := make(chan struct{})
	newDone := make(chan struct{})

	go func() {
		defer close(newDone)

		group.DoCall("key", newCall, func(state CallState) (int, error) {
			if state.Forgotten() {
				return 0, errors.New("new call is unexpectedly forgotten")
			}

			close(newStarted)
			<-newRelease

			return 2, nil
		})
	}()

	waitClosed(t, newStarted)

	var oldForgotten atomic.Bool
	group.DoCall("key", oldCall, func(state CallState) (int, error) {
		oldForgotten.Store(state.Forgotten())
		return 1, nil
	})

	if !oldForgotten.Load() {
		t.Fatal("old wave did not observe Forget")
	}

	joinedNew, shouldLoad := group.StartCall("key")
	if shouldLoad {
		t.Fatal("old wave completion removed the new active wave")
	}

	oldResult, err := oldCall.Wait(context.Background())
	if err != nil || oldResult.Err != nil || oldResult.Val != 1 {
		t.Fatalf("old result = %+v, wait error = %v", oldResult, err)
	}

	close(newRelease)
	waitClosed(t, newDone)

	for index, call := range []Call[int]{newCall, joinedNew} {
		result, err := call.Wait(context.Background())
		if err != nil || result.Err != nil || result.Val != 2 || !result.Shared {
			t.Fatalf("new caller %d result = %+v, wait error = %v", index, result, err)
		}
	}
}

func TestForgetAllMarksEveryActiveWave(t *testing.T) {
	var group Group[string, int]

	first, firstShouldLoad := group.StartCall("first")
	second, secondShouldLoad := group.StartCall("second")
	if !firstShouldLoad || !secondShouldLoad {
		t.Fatal("initial calls must both be owners")
	}

	group.ForgetAll()

	var firstForgotten, secondForgotten atomic.Bool

	group.DoCall("first", first, func(state CallState) (int, error) {
		firstForgotten.Store(state.Forgotten())
		return 1, nil
	})
	group.DoCall("second", second, func(state CallState) (int, error) {
		secondForgotten.Store(state.Forgotten())
		return 2, nil
	})

	if !firstForgotten.Load() || !secondForgotten.Load() {
		t.Fatal("ForgetAll() was not visible to every active wave")
	}

	firstResult, firstErr := first.Wait(context.Background())
	secondResult, secondErr := second.Wait(context.Background())

	if firstErr != nil || firstResult.Err != nil || firstResult.Val != 1 {
		t.Fatalf("first result = %+v, wait error = %v", firstResult, firstErr)
	}
	if secondErr != nil || secondResult.Err != nil || secondResult.Val != 2 {
		t.Fatalf("second result = %+v, wait error = %v", secondResult, secondErr)
	}
}

func TestPanicPropagatesToEveryWaiterAndCleansUp(t *testing.T) {
	var group Group[string, int]

	wantErr := errors.New("boom")

	owner, shouldLoad := group.StartCall("key")
	if !shouldLoad {
		t.Fatal("owner shouldLoad = false")
	}
	waiter, shouldLoad := group.StartCall("key")
	if shouldLoad {
		t.Fatal("waiter shouldLoad = true")
	}

	group.DoCall("key", owner, func(CallState) (int, error) {
		panic(wantErr)
	})

	for index, call := range []Call[int]{owner, waiter} {
		var recovered any

		func() {
			defer func() {
				recovered = recover()
			}()

			_, _ = call.Wait(context.Background())
		}()

		if recovered == nil {
			t.Fatalf("caller %d did not observe panic", index)
		}

		assertPanicError(t, recovered, wantErr)
	}

	next, shouldLoad := group.StartCall("key")
	if !shouldLoad {
		t.Fatal("panicking wave remained registered")
	}

	group.DoCall("key", next, func(CallState) (int, error) {
		return 42, nil
	})

	result, err := next.Wait(context.Background())
	if err != nil || result.Err != nil || result.Val != 42 || result.Shared {
		t.Fatalf("next result = %+v, wait error = %v", result, err)
	}
}

func TestGoexitPropagatesToEveryWaiterAndCleansUp(t *testing.T) {
	var group Group[string, int]

	owner, shouldLoad := group.StartCall("key")
	if !shouldLoad {
		t.Fatal("owner shouldLoad = false")
	}
	waiter, shouldLoad := group.StartCall("key")
	if shouldLoad {
		t.Fatal("waiter shouldLoad = true")
	}

	doCallDone := make(chan struct{})
	var doCallReturned atomic.Bool

	go func() {
		defer close(doCallDone)

		group.DoCall("key", owner, func(CallState) (int, error) {
			runtime.Goexit()
			return 0, nil
		})

		doCallReturned.Store(true)
	}()

	waitClosed(t, doCallDone)

	if doCallReturned.Load() {
		t.Fatal("DoCall returned after runtime.Goexit")
	}

	for index, call := range []Call[int]{owner, waiter} {
		done := make(chan struct{})
		var returned atomic.Bool

		go func(call Call[int]) {
			defer close(done)

			_, _ = call.Wait(context.Background())
			returned.Store(true)
		}(call)

		waitClosed(t, done)

		if returned.Load() {
			t.Fatalf("caller %d returned after runtime.Goexit", index)
		}
	}

	next, shouldLoad := group.StartCall("key")
	if !shouldLoad {
		t.Fatal("Goexit wave remained registered")
	}

	group.DoCall("key", next, func(CallState) (int, error) {
		return 42, nil
	})

	result, err := next.Wait(context.Background())
	if err != nil || result.Err != nil || result.Val != 42 || result.Shared {
		t.Fatalf("next result = %+v, wait error = %v", result, err)
	}
}

func TestDoCallRejectsZeroCall(t *testing.T) {
	var group Group[string, int]
	var call Call[int]

	defer func() {
		if recover() == nil {
			t.Fatal("DoCall with zero Call did not panic")
		}
	}()

	group.DoCall("key", call, func(CallState) (int, error) {
		return 1, nil
	})
}

func TestZeroCallWaitReturnsCanceled(t *testing.T) {
	var call Call[int]

	result, err := call.Wait(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context.Canceled", err)
	}
	if result != (Result[int]{}) {
		t.Fatalf("Wait() result = %+v, want zero result", result)
	}
}

func TestPanicErrorUnwrapsOnlyErrors(t *testing.T) {
	sentinel := errors.New("boom")

	withError := newPanicError(sentinel)
	if !errors.Is(withError, sentinel) {
		t.Fatalf("errors.Is(%v, sentinel) = false", withError)
	}

	withoutError := newPanicError("boom")
	if withoutError.Unwrap() != nil {
		t.Fatalf("Unwrap() = %v, want nil", withoutError.Unwrap())
	}
	if withoutError.Error() == "" {
		t.Fatal("Error() is empty")
	}
}

func assertPanicError(t *testing.T, recovered any, want error) {
	t.Helper()

	panicErr, ok := recovered.(*panicError)
	if !ok {
		t.Fatalf(
			"panic = %T(%v), want *panicError",
			recovered,
			recovered,
		)
	}

	if !errors.Is(panicErr, want) {
		t.Fatalf(
			"panic error = %v, want errors.Is(..., %v)",
			panicErr,
			want,
		)
	}

	if len(panicErr.stack) == 0 {
		t.Fatal("panic stack is empty")
	}
}

func waitClosed(t *testing.T, channel <-chan struct{}) {
	t.Helper()

	select {
	case <-channel:
	case <-time.After(singleflightTestTimeout):
		t.Fatal("timed out waiting for channel")
	}
}

func waitGroup(t *testing.T, group *sync.WaitGroup) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()

	waitClosed(t, done)
}
