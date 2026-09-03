// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// errGoexit indicates runtime.Goexit was called in the user-given function.
var errGoexit = errors.New("runtime.Goexit was called")

// panicError is an arbitrary value recovered from a panic together with the
// stack trace captured while executing the user-given function.
type panicError struct {
	value any
	stack []byte
}

func (panicErr *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", panicErr.value, panicErr.stack)
}

func (panicErr *panicError) Unwrap() error {
	err, ok := panicErr.value.(error)
	if !ok {
		return nil
	}

	return err
}

func newPanicError(value any) *panicError {
	stack := debug.Stack()

	// The first line is "goroutine N [status]:", which is not useful when the
	// panic is later observed through a shared call handle.
	if line := bytes.IndexByte(stack, '\n'); line >= 0 {
		stack = stack[line+1:]
	}

	return &panicError{value: value, stack: stack}
}

// CallState exposes state associated with one active singleflight call.
// It remains valid only until that call completes.
type CallState struct {
	forgotten *atomic.Bool
}

// Forgotten reports whether Forget or ForgetAll superseded this call while it
// was in flight.
func (state CallState) Forgotten() bool {
	return state.forgotten != nil && state.forgotten.Load()
}

// Result is the value produced by one singleflight call.
type Result[V any] struct {
	Val    V
	Err    error
	Shared bool
}

// Call is a lightweight handle to one singleflight wave.
//
// Every caller in the same wave observes one shared completion signal. Waiting
// callers may stop waiting independently when their contexts are canceled.
type Call[V any] struct {
	call *call[V]
}

// Wait waits for the shared result or for ctx to be canceled. A panic or
// runtime.Goexit from the owner is propagated to every caller that waits for
// the shared result.
func (handle Call[V]) Wait(ctx context.Context) (Result[V], error) {
	current := handle.call
	if current == nil {
		return Result[V]{}, context.Canceled
	}

	select {
	case <-ctx.Done():
		return Result[V]{}, ctx.Err()

	case <-current.done:

		if panicErr, ok := errors.AsType[*panicError](current.err); ok {
			panic(panicErr)
		}

		if errors.Is(current.err, errGoexit) {
			runtime.Goexit()
		}

		return Result[V]{
			Val:    current.val,
			Err:    current.err,
			Shared: current.dups > 0,
		}, nil
	}
}

type call[V any] struct {
	done chan struct{}

	// val and err are written before done is closed and read only after a
	// waiter observes that close.
	val V
	err error

	// dups is mutated while Group.mu is held before completion. forgotten may
	// be set by Forget or ForgetAll while the owner is executing.
	dups      uint32
	forgotten atomic.Bool
}

// Group coalesces concurrent work for equal keys.
// A Group must not be copied after first use.
type Group[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*call[V]
}

// StartCall joins the active wave for key or registers a new one.
//
// owner is true only for the caller responsible for executing the shared
// work with DoCall.
func (group *Group[K, V]) StartCall(key K) (handle Call[V], owner bool) {
	group.mu.Lock()

	if group.m == nil {
		group.m = make(map[K]*call[V])
	}

	if current, ok := group.m[key]; ok {
		current.dups++
		group.mu.Unlock()

		return Call[V]{call: current}, false
	}

	current := &call[V]{
		done: make(chan struct{}),
	}
	group.m[key] = current

	group.mu.Unlock()

	return Call[V]{call: current}, true
}

// DoCall executes the shared work synchronously for a call returned by
// StartCall with owner=true.
//
// DoCall must be called exactly once for the owner call. Panics are captured so
// every caller in the wave can observe the same panic through Wait.
// runtime.Goexit completes the wave before terminating the executing goroutine.
func (group *Group[K, V]) DoCall(
	key K,
	handle Call[V],
	fn func(CallState) (V, error),
) {
	current := handle.call
	if current == nil {
		panic("singleflight: DoCall called with zero Call")
	}

	normalReturn := false
	panicRecovered := false

	// Double defer distinguishes panic from runtime.Goexit. Cleanup always runs
	// before the call completes.
	defer func() {
		if !normalReturn && !panicRecovered {
			current.err = errGoexit
		}

		group.finish(key, current)
	}()

	func() {
		defer func() {
			if !normalReturn {
				if value := recover(); value != nil {
					current.err = newPanicError(value)
				}
			}
		}()

		current.val, current.err = fn(
			CallState{forgotten: &current.forgotten},
		)
		normalReturn = true
	}()

	if !normalReturn {
		panicRecovered = true
	}
}

// Forget marks the active call for key as superseded and removes it from the
// group. Existing callers still finish their current wave; a later call for the
// same key starts a new wave.
func (group *Group[K, V]) Forget(key K) {
	group.mu.Lock()

	if current := group.m[key]; current != nil {
		current.forgotten.Store(true)
		delete(group.m, key)
	}

	group.mu.Unlock()
}

// ForgetAll marks every active call as superseded and removes it from the
// group. Existing callers still finish their current waves.
func (group *Group[K, V]) ForgetAll() {
	group.mu.Lock()

	for key, current := range group.m {
		current.forgotten.Store(true)
		delete(group.m, key)
	}

	group.mu.Unlock()
}

func (group *Group[K, V]) finish(key K, current *call[V]) {
	group.mu.Lock()

	// A forgotten old wave must never remove a newer wave for the same key.
	if group.m[key] == current {
		delete(group.m, key)
	}

	close(current.done)

	group.mu.Unlock()
}
