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

func TestDoReturnsValue(t *testing.T) {
	var group Group[string, int]

	wantErr := errors.New("new call is unexpectedly forgotten")

	call := group.Do("key", func(state CallState) (int, error) {
		if state.Forgotten() {
			return 0, wantErr
		}

		return 42, nil
	})

	result, err := call.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait() error = %v, want nil", err)
	}
	if result.Err != nil {
		t.Fatalf("result error = %v, want nil", result.Err)
	}
	if result.Val != 42 {
		t.Fatalf("result value = %d, want 42", result.Val)
	}
	if result.Shared {
		t.Fatal("result shared = true, want false")
	}
}

func TestDoReturnsLoaderError(t *testing.T) {
	var group Group[string, int]

	wantErr := errors.New("load failed")

	call := group.Do("key", func(CallState) (int, error) {
		return 0, wantErr
	})

	result, waitErr := call.Wait(context.Background())
	if waitErr != nil {
		t.Fatalf("Wait() error = %v, want nil", waitErr)
	}
	if !errors.Is(result.Err, wantErr) {
		t.Fatalf("result error = %v, want %v", result.Err, wantErr)
	}
	if result.Val != 0 {
		t.Fatalf("result value = %d, want 0", result.Val)
	}
	if result.Shared {
		t.Fatal("result shared = true, want false")
	}
}

func TestDoSuppressesManyCallersForSameKey(t *testing.T) {
	var group Group[string, int]

	const callers = 256

	start := make(chan struct{})
	release := make(chan struct{})
	registered := make(chan struct{}, callers)
	errorsChannel := make(chan error, callers)

	var (
		loaderCalls atomic.Int64
		waiters     sync.WaitGroup
	)

	waiters.Add(callers)

	for range callers {
		go func() {
			defer waiters.Done()
			<-start

			call := group.Do("key", func(CallState) (int, error) {
				loaderCalls.Add(1)
				<-release

				return 42, nil
			})

			registered <- struct{}{}

			result, err := call.Wait(context.Background())
			if err != nil {
				errorsChannel <- err
				return
			}
			if result.Err != nil {
				errorsChannel <- result.Err
				return
			}
			if result.Val != 42 {
				errorsChannel <- errors.New("unexpected result value")
				return
			}
			if !result.Shared {
				errorsChannel <- errors.New("result is unexpectedly unshared")
			}
		}()
	}

	close(start)

	for range callers {
		receive(t, registered)
	}

	waitForDuplicates(t, &group, "key", callers-1)
	close(release)

	waitGroup(t, &waiters)
	close(errorsChannel)

	for err := range errorsChannel {
		t.Fatal(err)
	}

	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
}

func TestDoKeepsDifferentKeysIndependent(t *testing.T) {
	var group Group[int, int]

	const (
		keys          = 64
		callersPerKey = 8
		totalCallers  = keys * callersPerKey
	)

	start := make(chan struct{})
	release := make(chan struct{})
	registered := make(chan struct{}, totalCallers)
	errorsChannel := make(chan error, totalCallers)

	calls := make([]atomic.Int64, keys)

	var waiters sync.WaitGroup
	waiters.Add(totalCallers)

	for key := range keys {
		for range callersPerKey {
			go func(key int) {
				defer waiters.Done()
				<-start

				call := group.Do(key, func(CallState) (int, error) {
					calls[key].Add(1)
					<-release

					return key, nil
				})

				registered <- struct{}{}

				result, err := call.Wait(context.Background())
				if err != nil {
					errorsChannel <- err
					return
				}
				if result.Err != nil {
					errorsChannel <- result.Err
					return
				}
				if result.Val != key {
					errorsChannel <- errors.New("cross-key result observed")
					return
				}
				if !result.Shared {
					errorsChannel <- errors.New("result is unexpectedly unshared")
				}
			}(key)
		}
	}

	close(start)

	for range totalCallers {
		receive(t, registered)
	}

	for key := range keys {
		waitForDuplicates(t, &group, key, callersPerKey-1)
	}

	close(release)
	waitGroup(t, &waiters)
	close(errorsChannel)

	for err := range errorsChannel {
		t.Fatal(err)
	}

	for key := range keys {
		if got := calls[key].Load(); got != 1 {
			t.Fatalf("loader calls for key %d = %d, want 1", key, got)
		}
	}
}

func TestWaitCancellationDoesNotCancelWave(t *testing.T) {
	var group Group[string, int]

	started := make(chan struct{})
	release := make(chan struct{})

	first := group.Do("key", func(CallState) (int, error) {
		close(started)
		<-release

		return 42, nil
	})

	waitClosed(t, started)

	second := group.Do("key", func(CallState) (int, error) {
		return 99, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := first.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context.Canceled", err)
	}

	close(release)

	result, err := second.Wait(context.Background())
	if err != nil {
		t.Fatalf("second Wait() error = %v, want nil", err)
	}
	if result.Err != nil {
		t.Fatalf("second result error = %v, want nil", result.Err)
	}
	if result.Val != 42 {
		t.Fatalf("second result value = %d, want 42", result.Val)
	}
	if !result.Shared {
		t.Fatal("second result shared = false, want true")
	}
}

func TestForgetMarksOldWaveAndStartsNewWave(t *testing.T) {
	var group Group[string, int]

	oldStarted := make(chan struct{})
	oldRelease := make(chan struct{})
	oldForgotten := make(chan bool, 1)

	oldCall := group.Do("key", func(state CallState) (int, error) {
		close(oldStarted)
		<-oldRelease
		oldForgotten <- state.Forgotten()

		return 1, nil
	})

	waitClosed(t, oldStarted)
	group.Forget("key")

	newForgotten := make(chan bool, 1)
	newCall := group.Do("key", func(state CallState) (int, error) {
		newForgotten <- state.Forgotten()

		return 2, nil
	})

	newResult, err := newCall.Wait(context.Background())
	if err != nil {
		t.Fatalf("new Wait() error = %v, want nil", err)
	}
	if newResult.Err != nil || newResult.Val != 2 || newResult.Shared {
		t.Fatalf("new result = %+v, want value=2 err=nil shared=false", newResult)
	}
	if receive(t, newForgotten) {
		t.Fatal("new wave inherited forgotten state")
	}

	close(oldRelease)

	oldResult, err := oldCall.Wait(context.Background())
	if err != nil {
		t.Fatalf("old Wait() error = %v, want nil", err)
	}
	if oldResult.Err != nil || oldResult.Val != 1 || oldResult.Shared {
		t.Fatalf("old result = %+v, want value=1 err=nil shared=false", oldResult)
	}
	if !receive(t, oldForgotten) {
		t.Fatal("old wave did not observe Forget")
	}
}

func TestOldForgottenWaveCannotDeleteNewWave(t *testing.T) {
	var group Group[string, int]

	oldStarted := make(chan struct{})
	oldRelease := make(chan struct{})

	oldCall := group.Do("key", func(CallState) (int, error) {
		close(oldStarted)
		<-oldRelease

		return 1, nil
	})

	waitClosed(t, oldStarted)

	group.mu.Lock()
	oldCurrent := group.m["key"]
	group.mu.Unlock()
	if oldCurrent == nil {
		t.Fatal("old wave is not registered")
	}

	group.Forget("key")

	newStarted := make(chan struct{})
	newRelease := make(chan struct{})

	newCall := group.Do("key", func(CallState) (int, error) {
		close(newStarted)
		<-newRelease

		return 2, nil
	})

	waitClosed(t, newStarted)

	group.mu.Lock()
	newCurrent := group.m["key"]
	group.mu.Unlock()
	if newCurrent == nil {
		t.Fatal("new wave is not registered")
	}
	if newCurrent == oldCurrent {
		t.Fatal("new wave reused old call")
	}

	close(oldRelease)

	oldResult, err := oldCall.Wait(context.Background())
	if err != nil || oldResult.Err != nil || oldResult.Val != 1 {
		t.Fatalf("old wave result = %+v, wait error = %v", oldResult, err)
	}

	group.mu.Lock()
	current := group.m["key"]
	group.mu.Unlock()
	if current != newCurrent {
		t.Fatal("old wave completion removed the new active wave")
	}

	close(newRelease)

	newResult, err := newCall.Wait(context.Background())
	if err != nil || newResult.Err != nil || newResult.Val != 2 {
		t.Fatalf("new wave result = %+v, wait error = %v", newResult, err)
	}
}

func TestForgetAllMarksEveryActiveWave(t *testing.T) {
	var group Group[string, int]

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	release := make(chan struct{})

	firstForgotten := make(chan bool, 1)
	secondForgotten := make(chan bool, 1)

	first := group.Do("first", func(state CallState) (int, error) {
		close(firstStarted)
		<-release
		firstForgotten <- state.Forgotten()

		return 1, nil
	})

	second := group.Do("second", func(state CallState) (int, error) {
		close(secondStarted)
		<-release
		secondForgotten <- state.Forgotten()

		return 2, nil
	})

	waitClosed(t, firstStarted)
	waitClosed(t, secondStarted)

	group.ForgetAll()

	group.mu.Lock()
	active := len(group.m)
	group.mu.Unlock()
	if active != 0 {
		t.Fatalf("active calls after ForgetAll() = %d, want 0", active)
	}

	close(release)

	firstResult, firstErr := first.Wait(context.Background())
	secondResult, secondErr := second.Wait(context.Background())

	if firstErr != nil || firstResult.Err != nil || firstResult.Val != 1 {
		t.Fatalf("first result = %+v, wait error = %v", firstResult, firstErr)
	}
	if secondErr != nil || secondResult.Err != nil || secondResult.Val != 2 {
		t.Fatalf("second result = %+v, wait error = %v", secondResult, secondErr)
	}
	if !receive(t, firstForgotten) {
		t.Fatal("first wave did not observe ForgetAll")
	}
	if !receive(t, secondForgotten) {
		t.Fatal("second wave did not observe ForgetAll")
	}
}

func TestGoexitPropagatesToEveryWaiterAndCleansUp(t *testing.T) {
	var group Group[string, int]

	started := make(chan struct{})
	release := make(chan struct{})

	first := group.Do("key", func(CallState) (int, error) {
		close(started)
		<-release
		runtime.Goexit()

		return 0, nil
	})

	waitClosed(t, started)

	second := group.Do("key", func(CallState) (int, error) {
		return 99, nil
	})

	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	var firstReturned atomic.Bool
	var secondReturned atomic.Bool

	go func() {
		defer close(firstDone)
		_, _ = first.Wait(context.Background())
		firstReturned.Store(true)
	}()

	go func() {
		defer close(secondDone)
		_, _ = second.Wait(context.Background())
		secondReturned.Store(true)
	}()

	close(release)

	waitClosed(t, firstDone)
	waitClosed(t, secondDone)

	if firstReturned.Load() {
		t.Fatal("first waiter returned after runtime.Goexit")
	}
	if secondReturned.Load() {
		t.Fatal("second waiter returned after runtime.Goexit")
	}

	next := group.Do("key", func(CallState) (int, error) {
		return 42, nil
	})
	result, err := next.Wait(context.Background())
	if err != nil || result.Err != nil || result.Val != 42 || result.Shared {
		t.Fatalf("next wave result = %+v, wait error = %v", result, err)
	}
}

func TestPanicPropagatesToEveryWaiterAndCleansUp(t *testing.T) {
	var group Group[string, int]

	wantErr := errors.New("boom")
	started := make(chan struct{})
	release := make(chan struct{})

	first := group.Do("key", func(CallState) (int, error) {
		close(started)
		<-release
		panic(wantErr)
	})

	waitClosed(t, started)

	second := group.Do("key", func(CallState) (int, error) {
		return 99, nil
	})

	close(release)

	for index, call := range []Call[int]{first, second} {
		var recovered any
		func() {
			defer func() {
				recovered = recover()
			}()

			_, _ = call.Wait(context.Background())
		}()

		if recovered == nil {
			t.Fatalf("waiter %d did not observe loader panic", index)
		}
		assertPanicError(t, recovered, wantErr)
	}

	select {
	case <-first.call.done:
	default:
		t.Fatal("panicking wave did not close its completion signal")
	}

	group.mu.Lock()
	_, active := group.m["key"]
	group.mu.Unlock()
	if active {
		t.Fatal("panicking wave remained registered")
	}

	next := group.Do("key", func(CallState) (int, error) {
		return 42, nil
	})
	result, err := next.Wait(context.Background())
	if err != nil || result.Err != nil || result.Val != 42 || result.Shared {
		t.Fatalf("next wave result = %+v, wait error = %v", result, err)
	}
}

func TestPanicAfterWaitCancellationDoesNotEscapeWorker(t *testing.T) {
	var group Group[string, int]

	started := make(chan struct{})
	release := make(chan struct{})

	call := group.Do("key", func(CallState) (int, error) {
		close(started)
		<-release
		panic("boom")
	})

	waitClosed(t, started)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := call.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context.Canceled", err)
	}

	close(release)
	waitClosed(t, call.call.done)

	group.mu.Lock()
	_, active := group.m["key"]
	group.mu.Unlock()
	if active {
		t.Fatal("panicking wave remained registered")
	}

	next := group.Do("key", func(CallState) (int, error) {
		return 42, nil
	})
	result, err := next.Wait(context.Background())
	if err != nil || result.Err != nil || result.Val != 42 || result.Shared {
		t.Fatalf("next wave result = %+v, wait error = %v", result, err)
	}
}

func TestPanicValueAndStackArePreserved(t *testing.T) {
	var group Group[string, int]

	want := struct {
		code int
	}{code: 42}

	call := group.Do("key", func(CallState) (int, error) {
		panic(want)
	})

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()

		_, _ = call.Wait(context.Background())
	}()

	panicErr, ok := recovered.(*panicError)
	if !ok {
		t.Fatalf("panic = %T(%v), want *panicError", recovered, recovered)
	}
	if panicErr.value != want {
		t.Fatalf("panic value = %#v, want %#v", panicErr.value, want)
	}
	if len(panicErr.stack) == 0 {
		t.Fatal("panic stack is empty")
	}
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

func assertPanicError(t *testing.T, recovered any, want error) {
	t.Helper()

	panicErr, ok := recovered.(*panicError)
	if !ok {
		t.Fatalf("panic = %T(%v), want *panicError", recovered, recovered)
	}
	if !errors.Is(panicErr, want) {
		t.Fatalf("panic error = %v, want errors.Is(..., %v)", panicErr, want)
	}
	if len(panicErr.stack) == 0 {
		t.Fatal("panic stack is empty")
	}
}

func waitForDuplicates[K comparable, V any](
	t *testing.T,
	group *Group[K, V],
	key K,
	want int,
) {
	t.Helper()

	deadline := time.Now().Add(singleflightTestTimeout)
	for time.Now().Before(deadline) {
		group.mu.Lock()
		current := group.m[key]
		var duplicates uint32
		if current != nil {
			duplicates = current.dups
		}
		group.mu.Unlock()

		if duplicates >= uint32(want) {
			return
		}

		runtime.Gosched()
	}

	t.Fatalf("duplicates for key did not reach %d", want)
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

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()

	select {
	case value := <-channel:
		return value
	case <-time.After(singleflightTestTimeout):
		t.Fatal("timed out waiting for value")
	}

	var zero T
	return zero
}
