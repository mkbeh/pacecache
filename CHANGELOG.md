# Changelog

All notable changes to this project will be documented in this file.

## v1.2.0

This release streamlines cache removal and makes cache-aside loading configurable through default and per-call loaders.

### Added

* **Default Loader:** Added `NewWithDefaultLoader` to configure a key-aware loader once and reuse it across load
  operations.
* **Per-Call Loaders:** Added `GetOrLoadWith` and `GetOrLoadEntryWith` to use a loader supplied for a specific operation
  instead of the configured default loader.
* **Load Errors:** Added `ErrNoLoader` and `ErrNotInitialized` for explicit loader and cache initialization errors.

### Changed

* **Removal API:** Renamed `GetAndInvalidate`, `Invalidate`, and `InvalidateAll` to `GetAndDelete`, `Delete`, and
  `Clear`.
* **Removal Statistics:** Replaced invalidation counters with `DeletedEntryCount` and `ClearedEntryCount` to distinguish
  explicitly deleted entries from entries removed by full cache clears.
* **Clear Memory Reclamation:** `Clear` now discards retained entry-map storage so memory usage can scale with
  post-clear occupancy instead of historical cache size.
* **Loader API:** Made `Loader` key-aware; `GetOrLoad` and `GetOrLoadEntry` now use the loader configured for the cache.

## extra/paceotel/v1.2.0

This release aligns `paceotel` with the removal API introduced in `pacecache` v1.2.0.

### Changed

* **Removed Entries:** Renamed `pacecache.entry.invalidation.count` to `pacecache.entry.removed.count`.
* **Removal Operations:** Replaced `pacecache.invalidation.scope` with `pacecache.removal.operation`, using `delete`
  and `clear` as operation values.
* **Core Dependency:** Updated `github.com/mkbeh/pacecache` to v1.2.0.

## v1.1.0

This release establishes Go 1.27 as the new compatibility baseline for `pacecache`.

### Changed

* **Go Toolchain:** Raised the minimum supported Go version to 1.27 and updated project tooling accordingly.

## extra/paceotel/v1.1.0

This release aligns `paceotel` with the Go 1.27 baseline and `pacecache` v1.1.0.

### Changed

* **Go Toolchain:** Raised the minimum supported Go version to 1.27.
* **Core Dependency:** Updated `github.com/mkbeh/pacecache` to v1.1.0.

## v1.0.2

This patch release improves memory efficiency, non-expiring write performance, and loader coordination under concurrent
cache updates.

### Added

* **Cold-Fill Benchmark:** Added dedicated coverage for cache construction and full population to measure cold-start
  performance and allocation growth.

### Changed

* **Memory Efficiency:** Removed eager entry-map preallocation based on `MaxEntries`, so memory usage now scales with
  actual cache occupancy rather than the configured entry limit.
* **Non-Expiring Writes:** Skip unnecessary clock reads and deadline calculation when storing entries without time-based
  expiration.
* **Performance Benchmarks:** Refreshed throughput, hit-ratio, and memory results and redesigned benchmark charts for
  clearer comparison across cache sizes and workload mixes.

### Fixed

* **Loader Publication Ordering:** Prevented superseded in-flight `GetOrLoad` results from overwriting newer cache state
  or being published to waiting callers after a concurrent cache mutation wins the publication race.

## v1.0.1

This patch release focuses on reducing garbage collector scan work and improving benchmark stability.

### Changed

* **GC Performance Optimizations:** Reordered fields in cache entries and expiration buckets to reduce GC scan work
  without changing object sizes or allocation behavior.
* **Stable Benchmarks:** Fixed rare per-segment evictions during benchmark population caused by randomized hash
  distribution.

## v1.0.0

Initial production release of the core `pacecache` module.

### Added

* **Concurrent Storage:** Bounded, generic in-memory cache with a single storage segment by default and configurable
  segmentation for reducing lock contention under concurrent workloads.
* **LRU Eviction:** Exact least-recently-used eviction enforced independently within each configured storage segment.
* **Flexible Expiration:** Cache-level and per-entry TTLs, sliding expiration, explicit TTL refreshes, expiration
  jitter, and entries without time-based expiration.
* **Cache-Aside Loading:** Context-aware loading with concurrent same-key miss coalescing and publication barriers that
  prevent stale in-flight loads from overwriting newer cache state.
* **Concurrency-Safe Operations:** Includes `GetOrSet`, `GetOrSetEntry`, `GetAndInvalidate`, multi-key invalidation, and
  existence checks that do not update LRU recency or sliding expiration.
* **Expiration Cleanup:** Lazy expiration, explicit reclamation of expired entries, and optional periodic background
  cleanup.
* **Statistics & Observability:** Runtime statistics covering cache state, hits and misses, loader outcomes, evictions,
  expirations, invalidations, cleanup activity, and segment count.
* **Tooling:** Performance benchmarks covering throughput, hit ratio, and memory consumption, together with runnable
  basic and OpenTelemetry examples.

## extra/paceotel/v1.0.1

Initial release of the `paceotel` extension module.

### Added

* **OpenTelemetry Metrics:** Optional metrics integration covering cache capacity, size, lookup outcomes, loader
  outcomes, invalidations, cleanup activity, evictions, expirations, and segment count.
* **Metric Attributes:** Measurements enriched with attributes such as `pacecache.name`, `pacecache.lookup.result`,
  `pacecache.load.result`, and `pacecache.invalidation.scope` for filtering and aggregation.
* **Meter Provider Integration:** Support for an application-provided `metric.MeterProvider` through
  `WithMeterProvider`.
* **Exporter-Agnostic Design:** OpenTelemetry SDK and exporter configuration remain application concerns, allowing the
  integration to work with Prometheus, OTLP, and other exporters.