# pacecache

[//]: # (<img align="right" width="300" src="https://github.com/user-attachments/assets/1be9337c-2a56-44ed-9754-579933f1b4e7" alt="pacecache">)
<img align="right" width="300" src="tmp/assets/logo_v12.webp" alt="pacecache">

**Fast and concurrent in-memory cache for Go**

[![Go Reference](https://pkg.go.dev/badge/github.com/mkbeh/xredis.svg)](https://pkg.go.dev/github.com/mkbeh/xredis)
[![Go](https://github.com/mkbeh/xredis/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/mkbeh/xredis/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/mkbeh/xredis/branch/main/graph/badge.svg)](https://codecov.io/gh/mkbeh/xredis)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](LICENSE)

`pacecache` is a bounded, generic in-process Go cache for highly concurrent workloads. It combines segmented storage,
LRU eviction, flexible expiration, cache-aside loading, negative caching, and optional observability while keeping
common hot paths allocation-free.

The library provides an intuitive API with predictable behavior under high concurrency and contention.

<br clear="right">

## Features

* **Built for Concurrency:** Uses segmented storage to minimize lock contention under heavy load.
* **Bounded LRU Eviction:** Enforces exact LRU eviction within each segment with configurable cache capacity.
* **Flexible Expiration:** Supports default and per-entry TTLs, `NoExpiration`, jitter, sliding expiration, and explicit
  TTL refreshes.
* **Cache-Aside Loading:** Coalesces concurrent misses for the same key via `GetOrLoad` to prevent cache stampedes.
* **Negative Caching:** Caches "not-found" results independently to protect upstream data sources from repeated misses.
* **Concurrency-Safe Updates:** Prevents stale in-flight loads from overwriting newer cache state.
* **Expiration Cleanup:** Supports lazy expiration, explicit cleanup, and an optional background cleanup worker.*
  **Decoupled Observability:** Provides built-in statistics and optional OpenTelemetry metrics without coupling the core
  package to OpenTelemetry.
* **Allocation-Free Hot Paths:** Avoids per-operation allocations for common reads, updates, and expiration operations.

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

Create a cache by choosing its capacity and expiration policy. A cache has a logical name used by diagnostics and
metrics.

```go
users, err := pacecache.New[int64, User](
"users",
pacecache.WithMaxEntries(100_000),
pacecache.WithTTL(5*time.Minute),
pacecache.WithNegativeTTL(30*time.Second),
pacecache.WithJitter(30*time.Second),
)
if err != nil {
return err
}
defer users.Close()
```

`WithTTL` defines the default lifetime of positive entries. Negative results use their own TTL, while jitter spreads
positive expiration deadlines to avoid many entries expiring at the same instant.

Values can be written directly using the cache default TTL, a custom TTL, or no time-based expiration:

```go
// Use the cache-level TTL.
users.Set(42, user, pacecache.DefaultExpiration)

// Override the TTL for this entry.
users.Set(100, user, 30*time.Second)

// Keep the entry until it is evicted or invalidated.
users.Set(200, user, pacecache.NoExpiration)
```

A local lookup distinguishes between a positive hit, a cached negative result, and a regular miss:

```go
user, status := users.Get(42)

switch status {
case pacecache.LookupHit:
fmt.Println("cache hit:", user.Name)

case pacecache.LookupNegativeHit:
fmt.Println("user is known to be absent")

case pacecache.LookupMiss:
fmt.Println("user is not cached")
}
```

This distinction is useful when negative caching is enabled: a cached "not found" result is different from a key that
has never been loaded or has already expired.

If only the presence of a live positive entry matters, use `Exists`:

```go
if users.Exists(42) {
// A live positive entry exists.
}
```

### Cache-aside loading

For most application caches, `GetOrLoad` is the primary read path. It checks the cache first and invokes the loader only
when no live cached result exists.

A typical repository-backed lookup can be written as:

```go
func getUser(ctx context.Context, id int64) (User, bool, error) {
return users.GetOrLoad(
ctx,
id,
func(ctx context.Context) (User, bool, error) {
user, err := userRepository.FindByID(ctx, id)
if errors.Is(err, sql.ErrNoRows) {
// A successful not-found result can be negatively cached.
return User{}, false, nil
}
if err != nil {
// Loader errors are returned and are never cached.
return User{}, false, err
}

return user, true, nil
},
)
}
```

The returned `found` value describes whether the underlying value exists:

```go
user, found, err := getUser(ctx, 42)
if err != nil {
return err
}
if !found {
return ErrUserNotFound
}

fmt.Println(user.Name)
```

Concurrent misses for the same key are coalesced into one loader execution. Other callers wait for the shared result
instead of issuing duplicate requests to the database or upstream service.

When `WithNegativeTTL` is configured, a loader result with `found=false` and `err=nil` is cached independently. This
prevents repeated requests for known-missing data from reaching the upstream system.

> [!NOTE]
> `Set` and invalidation act as publication barriers. An older in-flight load cannot overwrite cache state written or
invalidated after that load started.

### Expiration

Per-entry expiration can be combined with cache-level policies.

For workloads where frequently accessed entries should stay alive, enable sliding expiration:

```go
sessions, err := pacecache.New[string, Session](
"sessions",
pacecache.WithMaxEntries(50_000),
pacecache.WithTTL(30*time.Minute),
pacecache.WithSlidingExpiration(),
pacecache.WithJitter(2*time.Minute),
)
if err != nil {
return err
}
defer sessions.Close()
```

A successful positive `Get` now refreshes the entry expiration:

```go
session, status := sessions.Get(sessionID)
if status != pacecache.LookupHit {
return ErrSessionNotFound
}

return session
```

Expiration can also be refreshed explicitly without enabling sliding expiration:

```go
if !sessions.RefreshTTL(sessionID) {
// The entry is missing, expired, or negatively cached.
}
```

`NoExpiration` entries do not expire by time, while negative entries are not refreshed by sliding expiration.

### Invalidation

Remove specific entries when their source data changes:

```go
users.Invalidate(42)
```

Multiple keys can be invalidated in one call:

```go
users.Invalidate(42, 100, 200)
```

Or clear the entire cache:

```go
users.InvalidateAll()
```

Invalidation does not need to cancel an already running loader. The loader may still return its result to callers
already waiting for it, but that stale result cannot repopulate the invalidated cache entry.

### Expiration cleanup

Expiration correctness does not depend on a background worker. Once an entry expires, it is never returned even if its
storage has not yet been reclaimed.

Expired entries are removed lazily as they are encountered and can also be reclaimed manually:

```go
removed := users.CleanupExpired()

log.Printf("removed %d expired entries", removed)
```

For caches with a large working set or entries that may expire without being accessed again, periodic cleanup can be
enabled:

```go
users, err := pacecache.New[int64, User](
"users",
pacecache.WithMaxEntries(100_000),
pacecache.WithTTL(5*time.Minute),
pacecache.WithCleanupInterval(time.Minute),
)
if err != nil {
return err
}
defer users.Close()
```

Background cleanup is opt-in. `Close` stops the cleanup worker and releases any other background resources associated
with the cache.

### Statistics

`Stats` returns a snapshot of the cache state and activity:

```go
stats := users.Stats()

log.Printf(
"entries=%d hits=%d negative_hits=%d misses=%d evictions=%d",
stats.EntryCount,
stats.HitCount,
stats.NegativeHitCount,
stats.MissCount,
stats.EvictionCount,
)
```

Statistics also include loader outcomes, shared loads, invalidations, and expirations, and can be exported through the
optional OpenTelemetry integration.
