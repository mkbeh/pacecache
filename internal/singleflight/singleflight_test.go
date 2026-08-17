package singleflight

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoReturnsValue(t *testing.T) {
	var group Group[string, int]

	value, err, shared := group.Do("key", func() (int, error) {
		return 42, nil
	})
	if err != nil || shared || value != 42 {
		t.Fatalf("Do() = (%d, %v, %t), want (42, nil, false)", value, err, shared)
	}
}

func TestDoSuppressesDuplicateCalls(t *testing.T) {
	var group Group[string, int]
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64

	type result struct {
		value  int
		err    error
		shared bool
	}

	first := make(chan result, 1)
	go func() {
		value, err, shared := group.Do("key", func() (int, error) {
			calls.Add(1)
			close(started)
			<-release
			return 42, nil
		})
		first <- result{value: value, err: err, shared: shared}
	}()

	<-started

	second := make(chan result, 1)
	go func() {
		value, err, shared := group.Do("key", func() (int, error) {
			calls.Add(1)
			return 99, nil
		})
		second <- result{value: value, err: err, shared: shared}
	}()

	waitForDuplicates(t, &group, "key", 1)
	close(release)

	for index, channel := range []<-chan result{first, second} {
		got := <-channel
		if got.err != nil || !got.shared || got.value != 42 {
			t.Fatalf("result %d = (%d, %v, %t), want (42, nil, true)", index, got.value, got.err, got.shared)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("function calls = %d, want 1", got)
	}
}

func TestDoChanSuppressesDuplicateCalls(t *testing.T) {
	var group Group[int64, string]
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64

	first := group.DoChan(42, func(CallState) (string, error) {
		calls.Add(1)
		close(started)
		<-release
		return "Ada", nil
	})

	<-started

	second := group.DoChan(42, func(CallState) (string, error) {
		calls.Add(1)
		return "wrong", nil
	})

	close(release)

	for index, channel := range []<-chan Result[string]{first, second} {
		got := <-channel
		if got.Err != nil || !got.Shared || got.Val != "Ada" {
			t.Fatalf("result %d = (%q, %v, %t), want (Ada, nil, true)", index, got.Val, got.Err, got.Shared)
		}

		select {
		case _, ok := <-channel:
			if !ok {
				t.Fatal("DoChan result channel was closed")
			}
			t.Fatal("DoChan result channel produced more than one result")
		default:
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("function calls = %d, want 1", got)
	}
}

func TestDoChanForgetPreservesExistingWaiters(t *testing.T) {
	var group Group[string, int]

	started := make(chan struct{})
	release := make(chan struct{})
	forgotten := make(chan bool, 1)

	var calls atomic.Int64

	first := group.DoChan("key", func(state CallState) (int, error) {
		calls.Add(1)
		close(started)

		<-release
		forgotten <- state.Forgotten()

		return 42, nil
	})

	<-started

	second := group.DoChan("key", func(CallState) (int, error) {
		calls.Add(1)
		return 99, nil
	})

	group.Forget("key")
	close(release)

	if !<-forgotten {
		t.Fatal("active call did not observe Forget")
	}

	for index, resultChannel := range []<-chan Result[int]{first, second} {
		result := <-resultChannel
		if result.Err != nil || result.Val != 42 || !result.Shared {
			t.Fatalf(
				"result %d = (%d, %v, %t), want (42, nil, true)",
				index,
				result.Val,
				result.Err,
				result.Shared,
			)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("function calls = %d, want 1", got)
	}
}

func TestDoChanForgetAllowsNewCall(t *testing.T) {
	var group Group[string, int]

	started := make(chan struct{})
	release := make(chan struct{})
	firstForgotten := make(chan bool, 1)
	secondForgotten := make(chan bool, 1)

	var calls atomic.Int64

	first := group.DoChan("key", func(state CallState) (int, error) {
		calls.Add(1)
		close(started)

		<-release
		firstForgotten <- state.Forgotten()

		return 42, nil
	})

	<-started
	group.Forget("key")

	second := group.DoChan("key", func(state CallState) (int, error) {
		calls.Add(1)
		secondForgotten <- state.Forgotten()
		return 99, nil
	})

	secondResult := <-second
	if secondResult.Err != nil || secondResult.Val != 99 || secondResult.Shared {
		t.Fatalf(
			"second result = (%d, %v, %t), want (99, nil, false)",
			secondResult.Val,
			secondResult.Err,
			secondResult.Shared,
		)
	}
	if <-secondForgotten {
		t.Fatal("new call is unexpectedly forgotten")
	}

	close(release)

	if !<-firstForgotten {
		t.Fatal("first call did not observe Forget")
	}

	firstResult := <-first
	if firstResult.Err != nil || firstResult.Val != 42 || firstResult.Shared {
		t.Fatalf(
			"first result = (%d, %v, %t), want (42, nil, false)",
			firstResult.Val,
			firstResult.Err,
			firstResult.Shared,
		)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("function calls = %d, want 2", got)
	}
}

func TestDoChanForgetAllMarksActiveCallsForgotten(t *testing.T) {
	var group Group[string, int]

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	release := make(chan struct{})

	firstForgotten := make(chan bool, 1)
	secondForgotten := make(chan bool, 1)

	first := group.DoChan("first", func(state CallState) (int, error) {
		close(firstStarted)
		<-release

		firstForgotten <- state.Forgotten()
		return 1, nil
	})

	second := group.DoChan("second", func(state CallState) (int, error) {
		close(secondStarted)
		<-release

		secondForgotten <- state.Forgotten()
		return 2, nil
	})

	<-firstStarted
	<-secondStarted

	group.ForgetAll()
	close(release)

	if !<-firstForgotten {
		t.Fatal("first call did not observe ForgetAll")
	}
	if !<-secondForgotten {
		t.Fatal("second call did not observe ForgetAll")
	}

	firstResult := <-first
	if firstResult.Err != nil || firstResult.Val != 1 {
		t.Fatalf("first result = (%d, %v), want (1, nil)", firstResult.Val, firstResult.Err)
	}

	secondResult := <-second
	if secondResult.Err != nil || secondResult.Val != 2 {
		t.Fatalf("second result = (%d, %v), want (2, nil)", secondResult.Val, secondResult.Err)
	}
}

func TestForgetDoesNotLetOldCallDeleteNewCall(t *testing.T) {
	var group Group[string, int]
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	firstDone := make(chan struct{})

	go func() {
		defer close(firstDone)
		_, _, _ = group.Do("key", func() (int, error) {
			close(firstStarted)
			<-firstRelease
			return 1, nil
		})
	}()

	<-firstStarted

	group.mu.Lock()
	oldCall := group.m["key"]
	group.mu.Unlock()
	if oldCall == nil {
		t.Fatal("old call is not registered")
	}

	group.Forget("key")

	secondStarted := make(chan struct{})
	secondRelease := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		_, _, _ = group.Do("key", func() (int, error) {
			close(secondStarted)
			<-secondRelease
			return 2, nil
		})
	}()

	<-secondStarted

	group.mu.Lock()
	newCall := group.m["key"]
	group.mu.Unlock()
	if newCall == nil || newCall == oldCall {
		t.Fatal("new call was not registered after Forget")
	}

	close(firstRelease)
	<-firstDone

	group.mu.Lock()
	current := group.m["key"]
	group.mu.Unlock()
	if current != newCall {
		t.Fatal("old call deleted the newer in-flight call")
	}

	close(secondRelease)
	<-secondDone
}

func TestDoPropagatesPanic(t *testing.T) {
	var group Group[string, int]
	sentinel := errors.New("boom")

	defer func() {
		recovered := recover()
		panicErr, ok := recovered.(*panicError)
		if !ok {
			t.Fatalf("panic = %T(%v), want *panicError", recovered, recovered)
		}
		if !errors.Is(panicErr, sentinel) {
			t.Fatalf("panic error = %v, want sentinel", panicErr)
		}
		if len(panicErr.stack) == 0 {
			t.Fatal("panic stack is empty")
		}
	}()

	_, _, _ = group.Do("key", func() (int, error) {
		panic(sentinel)
	})
}

func TestPanicErrorUnwrapsOnlyErrors(t *testing.T) {
	sentinel := errors.New("boom")
	withError := newPanicError(sentinel).(*panicError)
	if !errors.Is(withError, sentinel) {
		t.Fatalf("errors.Is(%v, sentinel) = false", withError)
	}

	withoutError := newPanicError("boom").(*panicError)
	if withoutError.Unwrap() != nil {
		t.Fatalf("Unwrap() = %v, want nil", withoutError.Unwrap())
	}
	if withoutError.Error() == "" {
		t.Fatal("Error() is empty")
	}
}

func TestDoPropagatesGoexitToOwnerAndDuplicate(t *testing.T) {
	var group Group[string, int]
	started := make(chan struct{})
	release := make(chan struct{})

	ownerDone := make(chan struct{})
	ownerReturned := atomic.Bool{}
	go func() {
		defer close(ownerDone)
		_, _, _ = group.Do("key", func() (int, error) {
			close(started)
			<-release
			runtime.Goexit()
			return 0, nil
		})
		ownerReturned.Store(true)
	}()

	<-started

	duplicateDone := make(chan struct{})
	duplicateReturned := atomic.Bool{}
	go func() {
		defer close(duplicateDone)
		_, _, _ = group.Do("key", func() (int, error) {
			return 1, nil
		})
		duplicateReturned.Store(true)
	}()

	waitForDuplicates(t, &group, "key", 1)
	close(release)

	select {
	case <-ownerDone:
	case <-time.After(time.Second):
		t.Fatal("owner did not exit")
	}
	select {
	case <-duplicateDone:
	case <-time.After(time.Second):
		t.Fatal("duplicate did not exit")
	}

	if ownerReturned.Load() {
		t.Fatal("owner returned after runtime.Goexit")
	}
	if duplicateReturned.Load() {
		t.Fatal("duplicate returned after shared runtime.Goexit")
	}
}

func TestForgetUnregisteredKey(t *testing.T) {
	var group Group[struct{ ID int }, int]
	group.Forget(struct{ ID int }{ID: 42})
}

func TestForgetAllEmptyGroup(t *testing.T) {
	var group Group[string, int]
	group.ForgetAll()
}

func waitForDuplicates[K comparable, V any](
	t *testing.T,
	group *Group[K, V],
	key K,
	want uint32,
) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		group.mu.Lock()
		call := group.m[key]
		var dups uint32
		if call != nil {
			dups = call.dups
		}
		group.mu.Unlock()

		if dups >= want {
			return
		}

		runtime.Gosched()
	}

	t.Fatalf("duplicate count did not reach %d", want)
}
