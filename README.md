# pacecache

<img align="right" width="300" src="https://github.com/user-attachments/assets/0d80da22-bd60-4abd-b9e6-42838f39261c" alt="pacecache">

**Fast and concurrent in-memory cache for Go**

[![Go Reference](https://pkg.go.dev/badge/github.com/mkbeh/pacecache.svg)](https://pkg.go.dev/github.com/mkbeh/pacecache)
[![Test](https://github.com/mkbeh/pacecache/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/mkbeh/pacecache/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/mkbeh/pacecache/branch/main/graph/badge.svg)](https://codecov.io/gh/mkbeh/pacecache)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](LICENSE)

`pacecache` is a bounded, generic in-process cache for Go, built for highly concurrent workloads. It combines segmented
LRU storage, flexible expiration, cache-aside loading, and optional observability while keeping resident hot paths
allocation-free.

The library provides an intuitive API with predictable behavior under high concurrency and contention.

## Features

* **Concurrency:** Segmented storage scales across concurrent workloads.
* **Generic API:** Type-safe caching with comparable keys and arbitrary value types.
* **Bounded LRU:** Exact per-segment LRU within a fixed total capacity.
* **Expiration:** Default and per-entry TTLs, jitter, sliding expiration, refresh, and no-expiration entries.
* **Cleanup:** Lazy expiration, explicit cleanup, and an optional background worker.
* **Cache-Aside:** Coalesces concurrent misses for the same key into a single load.
* **Safe Updates:** Publication barriers prevent stale loads from overwriting newer cache state.
* **Observability:** Built-in statistics with optional OpenTelemetry metrics.

## Installation

This repository contains the core `pacecache` module. The core package is released from the repository root:

```bash
go get github.com/mkbeh/pacecache
```

Optional OpenTelemetry metrics are available through `paceotel`:

```bash
go get github.com/mkbeh/pacecache/extra/paceotel
```

## Usage

Create a cache with `pacecache.New` and close it when it is no longer needed:

<!-- @formatter:off -->
```go
cache, err := pacecache.New[string, string]("cache")
if err != nil {
    panic(err)
}
defer cache.Close()
```
<!-- @formatter:on -->

Caches use a single storage segment by default. For highly concurrent workloads, use `WithSegmentCount` and benchmark
segment counts against the application's actual access pattern.

Entries do not expire by default. Use `WithTTL` to set the default expiration and `WithJitter` to spread expiration
deadlines and reduce synchronized expiration bursts. Individual entries can use the default TTL, a custom TTL, or
`NoExpiration`.

<!-- @formatter:off -->
```go
cache, _ := pacecache.New[string, string](
    "cache",
    pacecache.WithTTL(5*time.Minute),
    pacecache.WithJitter(30*time.Second),
)
defer cache.Close()
```
<!-- @formatter:on -->

Values can be stored, retrieved, checked, conditionally inserted, and removed:

<!-- @formatter:off -->
```go
// Store values with different expiration strategies.
cache.Set("key1", "value1", pacecache.DefaultExpiration) // cache-level TTL
cache.Set("key2", "value2", pacecache.NoExpiration)      // no expiration
cache.Set("key3", "value3", 30*time.Second)              // custom TTL

// Read a live value.
value, found := cache.Get("key1")

// Read a value together with its expiration metadata.
entry, found := cache.GetEntry("key1")

// Check existence without updating LRU or TTL.
exists := cache.Exists("key2")

// Atomically return an existing value or store a new one.
value, found = cache.GetOrSet("key4", "value4", pacecache.DefaultExpiration)
entry, found = cache.GetOrSetEntry("key5", "value5", 30*time.Second)

// Remove cached values.
value, found = cache.GetAndDelete("key3") // read and delete atomically
cache.Delete("key1")                      // delete one key
cache.Delete("key2", "key4")              // delete multiple keys
cache.DeleteExpired()                     // remove expired entries
cache.Clear()                             // remove all entries
```
<!-- @formatter:on -->

Use `GetOrLoadFunc` to load a value on a cache miss with a per-call loader. The loader runs only when no live entry
exists; successful results are cached using the cache's default expiration:

<!-- @formatter:off -->
```go
// Define a loader that fetches the value from an upstream source.
loader := pacecache.Loader[string, string](func(ctx context.Context, key string) (string, bool, error) {
    // Fetch data from a database, file, or remote service.
    return "loaded value", true, nil
})

// Return the cached value or invoke the loader on a miss.
value, found, err := cache.GetOrLoadFunc(ctx, "key", loader)
if err != nil {
    panic(err)
}

if found {
    fmt.Println("retrieved value:", value) // loaded key
}
```
<!-- @formatter:on -->

If the same loader is reused across calls, configure it once with `NewWithDefaultLoader` and use `GetOrLoad`:

<!-- @formatter:off -->
```go
cache, _ := pacecache.NewWithDefaultLoader[string, string](
    "cache",
    func(ctx context.Context, key string) (string, bool, error) {
        // Fetch data from a database, file, or remote service.
        return "loaded value", true, nil
    },
    pacecache.WithTTL(5*time.Minute),
)
defer cache.Close()

// Return the cached value or invoke the configured loader on a miss.
value, found, err := cache.GetOrLoad(ctx, "key")
if err != nil {
    panic(err)
}

if found {
    fmt.Println("retrieved value:", value) // loaded key
}
```
<!-- @formatter:on -->

Missing results and loader errors are returned without being cached. Concurrent misses for the same key share a single
loader execution, avoiding duplicate requests to the upstream source.

Expired entries are never returned and are removed lazily when encountered. Periodic background cleanup can be enabled
for entries that may remain untouched:

<!-- @formatter:off -->
```go
cache, _ := pacecache.New[string, string](
    "cache",
    pacecache.WithTTL(5*time.Minute),
    pacecache.WithCleanupInterval(time.Minute), // background cleanup
)
defer cache.Close()
```
<!-- @formatter:on -->

Background cleanup is optional. Expired entries can also be reclaimed explicitly with `DeleteExpired`. `Close` stops
the cleanup worker and waits for it to exit.
## Concurrency semantics

The cache coordinates concurrent loads and mutations to prevent duplicate upstream work and stale values from
overwriting newer cache state. The flow below shows how a shared in-flight load is handled when the cache is mutated
before the loader completes:

```mermaid
flowchart LR
    Request["Concurrent same-key GetOrLoad calls"]
    Miss["Cache miss"]
    Load["Single shared loader"]
    Check{"Cache changed<br/>while loading?"}
    Store["Store loaded value"]
    Reject["Reject stale result"]
    Request --> Miss
    Miss --> Load
    Load --> Check
    Check -->|No| Store
    Check -->|Yes| Reject
```

Concurrent misses for the same key share a single loader execution, while different keys are loaded independently. If
the cache is mutated while a load is in flight, the newer mutation takes precedence. The stale loaded value is discarded
instead of overwriting the newer cache state, and the loading call returns an error.

Callers waiting for a shared load can stop waiting through their own `context.Context` without blocking other waiting
callers.

## Observability

The cache provides built-in runtime statistics and optional OpenTelemetry metrics for monitoring cache behavior.

`Stats` returns a snapshot of the current cache state and cumulative activity:

<!-- @formatter:off -->
```go
// Retrieve a snapshot of the current cache state and activity.
stats := cache.Stats()

// Selected statistics available in the snapshot.
_ = stats.EntryCount        // Current resident entries
_ = stats.MaxEntries        // Configured maximum capacity
_ = stats.HitCount          // Cache hits
_ = stats.MissCount         // Cache misses
_ = stats.EvictionCount     // LRU evictions
_ = stats.ExpirationCount   // Expired entries removed
_ = stats.LoadErrorCount    // Loader errors
_ = stats.DeletedEntryCount // Explicitly deleted entries
_ = stats.ClearedEntryCount // Entries removed by full cache clears
```
<!-- @formatter:on -->

Statistics also include load outcomes, shared and superseded loads, deleted and cleared entry counts, cleanup activity,
and segment count.

Optional OpenTelemetry metrics are available through [paceotel](./extra/paceotel). OpenTelemetry configuration and
exporter selection remain application concerns, so Prometheus, OTLP, and other exporters can be used without changing
the cache integration.

For a complete setup, see the [example](./examples/otel).

## Performance

The benchmark suite evaluates concurrent throughput, cache hit ratio, and memory consumption under representative cache
workloads.

Benchmarks were run on an Intel Core i7-12700H (14 cores, 20 threads).

### Throughput

Measures concurrent read/write throughput using a pre-generated Scrambled Zipfian access pattern to create skewed key
access and hot-key contention.

* **Concurrency:** 8 parallel workers
* **Segments:** 512
* **Maximum entries:** 10K, 100K, and 1M
* **Write ratios:** 0%, 25%, 50%, 75%, and 100%
* **Expiration:** Disabled

![Throughput](./benchmarks/performance/throughput/assets/throughput.png)

### Hit Ratio

Measures how cache capacity affects hit ratio under a Zipfian access pattern.

* **Requests:** 1,000,000
* **Segments:** 1
* **Capacity:** 500 to 80K entries
* **Expiration:** Disabled to isolate capacity and eviction behavior

![Hit Ratio](./benchmarks/performance/hitratio/assets/hit-ratio.png)

### Memory

Measures live heap consumption after populating the cache with fixed-size keys and values.

* **Data:** Fixed 32-byte keys and 32-byte values
* **Segments:** 1
* **Capacity:** 1K to 1M entries
* **Expiration:** 1-hour TTL

![Memory Consumption](./benchmarks/performance/memory/assets/memory.png)

For the complete methodology, source code, and execution instructions, see the
[performance benchmarks](./benchmarks/performance).

## Examples

See the [examples](./examples) directory for runnable examples demonstrating how to use `pacecache`.

## License

This project is licensed under the [MIT License](LICENSE).
