// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Adapted from golang.org/x/sync/singleflight to support generic key and value types.

// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// errGoexit indicates runtime.Goexit was called in
// the user-given function.
var errGoexit = errors.New("runtime.Goexit was called")

// A panicError is an arbitrary value recovered from a panic
// with the stack trace during the execution of the given function.
type panicError struct {
	value any
	stack []byte
}

// Error implements error interface.
func (p *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

func (p *panicError) Unwrap() error {
	err, ok := p.value.(error)
	if !ok {
		return nil
	}

	return err
}

func newPanicError(v any) error {
	stack := debug.Stack()

	// The first line of the stack trace is of the form "goroutine N [status]:"
	// but by the time the panic reaches Do the goroutine may no longer exist
	// and its status will have changed. Trim out the misleading line.
	if line := bytes.IndexByte(stack[:], '\n'); line >= 0 {
		stack = stack[line+1:]
	}

	return &panicError{value: v, stack: stack}
}

// call is an in-flight or completed singleflight.Do call.
type call[V any] struct {
	wg sync.WaitGroup

	// These fields are written once before the WaitGroup is done
	// and are only read after the WaitGroup is done.
	val V
	err error

	// dups and chans are read and written with the singleflight mutex held
	// before the WaitGroup is done, and are read but not written after it is
	// done.
	dups uint32
	// forgotten is set when Forget or ForgetAll removes this call from the
	// active group before it completes. It remains reachable by the goroutine
	// executing the call and by callers already waiting for its result.
	forgotten atomic.Bool
	chans     []chan<- Result[V]
}

// CallState exposes the state of the singleflight call executing fn.
//
// A call is forgotten when Forget or ForgetAll removes it from the active
// group before fn completes. Future calls for the same key may then start new
// work independently of the forgotten call.
type CallState struct {
	forgotten *atomic.Bool
}

// Forgotten reports whether this call has been forgotten by the group.
func (state CallState) Forgotten() bool {
	return state.forgotten != nil && state.forgotten.Load()
}

// Group represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
type Group[K comparable, V any] struct {
	mu sync.Mutex     // protects m
	m  map[K]*call[V] // lazily initialized
}

// Result holds the results of Do, so they can be passed
// on a channel.
type Result[V any] struct {
	Val    V
	Err    error
	Shared bool
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// The return value shared indicates whether v was given to multiple callers.
func (g *Group[K, V]) Do(key K, fn func() (V, error)) (v V, err error, shared bool) {
	g.mu.Lock()

	if g.m == nil {
		g.m = make(map[K]*call[V])
	}

	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()

		c.wg.Wait()

		if e, ok := c.err.(*panicError); ok {
			panic(e)
		} else if c.err == errGoexit {
			runtime.Goexit()
		}

		return c.val, c.err, true
	}

	c := new(call[V])
	c.wg.Add(1)
	g.m[key] = c

	g.mu.Unlock()

	g.doCall(c, key, fn)

	return c.val, c.err, c.dups > 0
}

// DoChan is like Do but returns a channel that will receive the results when
// they are ready. The function executing the shared call receives its
// CallState so it can observe whether the call was forgotten before completion.
//
// The returned channel will not be closed.
func (g *Group[K, V]) DoChan(
	key K,
	fn func(CallState) (V, error),
) <-chan Result[V] {
	ch := make(chan Result[V], 1)

	g.mu.Lock()

	if g.m == nil {
		g.m = make(map[K]*call[V])
	}

	if c, ok := g.m[key]; ok {
		c.dups++
		c.chans = append(c.chans, ch)

		g.mu.Unlock()

		return ch
	}

	c := &call[V]{
		chans: []chan<- Result[V]{ch},
	}
	c.wg.Add(1)
	g.m[key] = c

	g.mu.Unlock()

	go g.doCallState(c, key, fn)

	return ch
}

// doCall handles a synchronous singleflight call for a key.
func (g *Group[K, V]) doCall(
	c *call[V],
	key K,
	fn func() (V, error),
) {
	normalReturn := false
	recovered := false

	// use double-defer to distinguish panic from runtime.Goexit,
	// more details see https://golang.org/cl/134395
	defer func() {
		// the given function invoked runtime.Goexit
		if !normalReturn && !recovered {
			c.err = errGoexit
		}

		g.mu.Lock()
		defer g.mu.Unlock()

		c.wg.Done()

		if g.m[key] == c {
			delete(g.m, key)
		}

		if e, ok := c.err.(*panicError); ok {
			// In order to prevent the waiting channels from being blocked forever,
			// needs to ensure that this panic cannot be recovered.
			if len(c.chans) > 0 {
				go panic(e)
				select {} // Keep this goroutine around so that it will appear in the crash dump.
			} else {
				panic(e)
			}
		} else if c.err == errGoexit {
			// Already in the process of goexit, no need to call again.
		} else {
			// Normal return.
			for _, ch := range c.chans {
				ch <- Result[V]{
					Val:    c.val,
					Err:    c.err,
					Shared: c.dups > 0,
				}
			}
		}
	}()

	func() {
		defer func() {
			if !normalReturn {
				// Ideally, we would wait to take a stack trace until we've determined
				// whether this is a panic or a runtime.Goexit.
				//
				// Unfortunately, the only way we can distinguish the two is to see
				// whether the recover stopped the goroutine from terminating, and by
				// the time we know that, the part of the stack trace relevant to the
				// panic has been discarded.
				if r := recover(); r != nil {
					c.err = newPanicError(r)
				}
			}
		}()

		c.val, c.err = fn()
		normalReturn = true
	}()

	if !normalReturn {
		recovered = true
	}
}

// doCallState handles an asynchronous singleflight call whose function needs
// to observe whether the call was forgotten while it was in flight.
func (g *Group[K, V]) doCallState(
	c *call[V],
	key K,
	fn func(CallState) (V, error),
) {
	normalReturn := false
	recovered := false

	// use double-defer to distinguish panic from runtime.Goexit,
	// more details see https://golang.org/cl/134395
	defer func() {
		// the given function invoked runtime.Goexit
		if !normalReturn && !recovered {
			c.err = errGoexit
		}

		g.mu.Lock()
		defer g.mu.Unlock()

		c.wg.Done()

		if g.m[key] == c {
			delete(g.m, key)
		}

		if e, ok := c.err.(*panicError); ok {
			// In order to prevent the waiting channels from being blocked forever,
			// needs to ensure that this panic cannot be recovered.
			if len(c.chans) > 0 {
				go panic(e)
				select {} // Keep this goroutine around so that it will appear in the crash dump.
			} else {
				panic(e)
			}
		} else if c.err == errGoexit {
			// Already in the process of goexit, no need to call again.
		} else {
			// Normal return.
			for _, ch := range c.chans {
				ch <- Result[V]{
					Val:    c.val,
					Err:    c.err,
					Shared: c.dups > 0,
				}
			}
		}
	}()

	func() {
		defer func() {
			if !normalReturn {
				// Ideally, we would wait to take a stack trace until we've determined
				// whether this is a panic or a runtime.Goexit.
				//
				// Unfortunately, the only way we can distinguish the two is to see
				// whether the recover stopped the goroutine from terminating, and by
				// the time we know that, the part of the stack trace relevant to the
				// panic has been discarded.
				if r := recover(); r != nil {
					c.err = newPanicError(r)
				}
			}
		}()

		c.val, c.err = fn(
			CallState{
				forgotten: &c.forgotten,
			},
		)
		normalReturn = true
	}()

	if !normalReturn {
		recovered = true
	}
}

// Forget tells the singleflight to forget about a key. Future calls to Do or
// DoChan for this key will start new work rather than waiting for an earlier
// call to complete. An active DoChan call observes the transition through its
// CallState.
func (g *Group[K, V]) Forget(key K) {
	g.mu.Lock()

	if c, ok := g.m[key]; ok {
		c.forgotten.Store(true)
		delete(g.m, key)
	}

	g.mu.Unlock()
}

// ForgetAll tells the singleflight to forget every active key. Future calls
// may start new work without waiting for calls that were active at the time of
// ForgetAll. Active DoChan calls observe the transition through their
// CallState.
func (g *Group[K, V]) ForgetAll() {
	g.mu.Lock()

	for _, c := range g.m {
		c.forgotten.Store(true)
	}

	clear(g.m)

	g.mu.Unlock()
}
