// Package pacecache provides a bounded, concurrent in-process cache.
//
// Cache keys may use any comparable Go type. Entries support TTL expiration,
// optional sliding expiration and TTL jitter, LRU eviction, cache-aside
// loading with configurable default or per-call loaders, duplicate load
// suppression, and explicit removal.
//
// Cache mutations act as publication barriers for concurrent loads, preventing
// superseded loader results from overwriting newer cache state.
//
// Cache statistics are collected locally and exposed through Cache.Stats.
// Optional metrics integrations register during New and observe those snapshots
// without adding telemetry calls to the cache request path.
//
// The cache is local to one application process. It does not provide
// distributed cache coherence between application instances.
package pacecache
