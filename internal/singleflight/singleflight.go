// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Adapted from golang.org/x/sync/singleflight to support generic key and value
// types, per-call publication state, caller cancellation, and one shared
// completion signal per asynchronous call wave.

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

func newPanicError(value any) error {
	stack := debug.Stack()

	// The first line is "goroutine N [status]:". By the time another caller
	// observes the panic that goroutine may no longer exist, so avoid retaining
	// a misleading status line.
	if line := bytes.IndexByte(stack, '\n'); line >= 0 {
		stack = stack[line+1:]
	}

	return &panicError{value: value, stack: stack}
}

// CallState exposes mutable state associated with one active singleflight call.
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
// Every caller in the same wave waits on one shared completion signal. Waiting
// may be canceled independently without canceling the worker or other callers.
type Call[V any] struct {
	call *call[V]
}

// Wait waits for the shared result or for ctx to be canceled. Panics and
// runtime.Goexit from the worker are propagated in the waiting caller.
func (registered Call[V]) Wait(ctx context.Context) (Result[V], error) {
	current := registered.call
	if current == nil {
		return Result[V]{}, context.Canceled
	}

	select {
	case <-ctx.Done():
		return Result[V]{}, ctx.Err()

	case <-current.done:
		if panicErr, ok := current.err.(*panicError); ok {
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
	// be set by Forget or ForgetAll while the worker is executing.
	dups      uint32
	forgotten atomic.Bool
}

// Group coalesces concurrent work for equal keys.
// A Group must not be copied after first use.
type Group[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*call[V]
}

// Do joins the active wave for key or starts one asynchronous worker.
//
// Do only registers the caller and returns a Call handle. The caller should
// wait with Call.Wait after releasing any external lock that protects
// registration ordering. Panics from fn are recovered by the worker and
// propagated by Wait in the waiting caller.
func (group *Group[K, V]) Do(
	key K,
	fn func(CallState) (V, error),
) Call[V] {
	current, owner := group.register(key)
	if owner {
		go group.doCall(key, current, fn)
	}

	return Call[V]{call: current}
}

// Forget marks the active call for key as superseded and removes it from the
// group. Existing callers still finish their current wave; a later Do for the
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

func (group *Group[K, V]) register(key K) (*call[V], bool) {
	group.mu.Lock()

	if group.m == nil {
		group.m = make(map[K]*call[V])
	}

	if current, ok := group.m[key]; ok {
		current.dups++
		group.mu.Unlock()

		return current, false
	}

	current := &call[V]{done: make(chan struct{})}
	group.m[key] = current

	group.mu.Unlock()

	return current, true
}

func (group *Group[K, V]) doCall(
	key K,
	current *call[V],
	fn func(CallState) (V, error),
) {
	normalReturn := false
	recovered := false

	// Double defer distinguishes panic from runtime.Goexit. Cleanup always runs
	// before the worker completes. Panics are stored on the call and propagated
	// by Wait in the waiting caller instead of escaping from this goroutine.
	defer func() {
		if !normalReturn && !recovered {
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
		recovered = true
	}
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
