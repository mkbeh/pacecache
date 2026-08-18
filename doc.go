// Package pacecache provides bounded in-process read-through caching.
//
// Cache keys may use any comparable Go type. Entries use TTL expiration,
// optional sliding expiration and TTL jitter, LRU eviction, duplicate load
// suppression, and explicit invalidation. Invalidation prevents loads started
// before an invalidation barrier from repopulating invalidated entries.
//
// Cache statistics are collected locally and exposed through Cache.Stats.
// Optional metrics integrations register during New and observe those snapshots
// without adding telemetry calls to the cache request path.
//
// The cache is local to one application process. It does not provide
// distributed cache coherence between application instances.
package pacecache
