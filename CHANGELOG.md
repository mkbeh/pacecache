# Changelog

All notable changes to this project will be documented in this file.

## v1.0.0 - 2026-08-20

Initial production release of the core `pacecache` module.

### Added

* **High-Concurrency Storage:** Bounded, generic in-memory cache with segmented storage designed to minimize lock
  contention under concurrent workloads.
* **LRU Eviction:** Exact least-recently-used eviction enforced independently within each cache segment.
* **Flexible Expiration:** Cache-level and per-entry TTLs, sliding expiration, explicit TTL refreshes, expiration
  jitter, and entries without time-based expiration.
* **Cache-Aside Loading:** Context-aware loading with concurrent same-key miss coalescing and protection against stale
  in-flight loads overwriting newer cache state.
* **Atomic Operations:** Concurrency-safe primitives including `GetOrSet`, `GetOrSetEntry`, `GetAndInvalidate`,
  multi-key invalidation, and non-mutating existence checks.
* **Expiration Cleanup:** Lazy expiration, explicit reclamation of expired entries, and optional periodic background
  cleanup.
* **Telemetry & Observability:** Runtime statistics covering cache state, hits and misses, loader outcomes, evictions,
  expirations, invalidations, cleanup activity, and segment count.
* **Tooling:** Performance benchmarks covering throughput, hit ratio, and memory consumption, together with runnable
  usage and OpenTelemetry examples.

---

## extra/paceotel/v1.0.0 - 2026-08-20

Initial release of the `paceotel` extension module.

### Added

* **OpenTelemetry Metrics:** Optional metrics integration covering cache capacity, size, lookup outcomes, loader
  outcomes, invalidations, cleanup activity, evictions, and expirations.
* **Metric Attributes:** Measurements enriched with attributes such as `lookup.result` and `load.result` for granular
  filtering and aggregation.
* **Meter Provider Integration:** Support for an application-provided `metric.MeterProvider` through
  `WithMeterProvider`.
* **Exporter-Agnostic Design:** OpenTelemetry SDK and exporter configuration remain application concerns, allowing the
  integration to work with Prometheus, OTLP, and other exporters.