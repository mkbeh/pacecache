package pacecache

import "errors"

// ErrLoadSuperseded indicates that a successful loader result was made stale
// by a newer Set, Invalidate, or InvalidateAll operation before it could be
// published to the cache.
var ErrLoadSuperseded = errors.New("pacecache: load superseded by cache mutation")
