package pacecache

import "errors"

var (
	// ErrNotInitialized indicates that an operation requires an initialized
	// cache but the cache is nil or has not been initialized.
	ErrNotInitialized = errors.New("pacecache: cache is not initialized")

	// ErrNoLoader indicates that an operation requires a loader but none is
	// available. NewWithLoader also returns ErrNoLoader when passed a nil loader.
	ErrNoLoader = errors.New("pacecache: loader is not configured")

	// ErrLoadSuperseded indicates that a successful loader result was made stale
	// by a newer cache mutation, such as Set, a GetOrSet or GetOrSetEntry insertion,
	// Delete, or Clear, before it could be published to the cache.
	ErrLoadSuperseded = errors.New("pacecache: load superseded by cache mutation")
)
