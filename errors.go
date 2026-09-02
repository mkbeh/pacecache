package pacecache

import "errors"

// ErrLoadSuperseded indicates that a successful loader result was made stale
// by a newer cache mutation, such as Set, a GetOrSet or GetOrSetEntry insertion,
// Delete, or Clear, before it could be published to the cache.
var ErrLoadSuperseded = errors.New("pacecache: load superseded by cache mutation")
