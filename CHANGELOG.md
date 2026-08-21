# Changelog

All notable changes to this project will be documented in this file.

## v1.0.0 - 2026-08-21

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

---

## extra/paceotel/v1.0.1 - 2026-08-21

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