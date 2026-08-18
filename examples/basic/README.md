# Basic cache

This example demonstrates a typical cache-aside workflow with `pacecache` and an in-memory user repository.

**It demonstrates:**

- Configuring a bounded generic cache with TTL, jitter, and negative caching
- Loading missing values through `GetOrLoad`
- Reading cached values and entry metadata with `Get` and `GetEntry`
- Caching not-found results with a negative TTL
- Invalidating cached data and reloading it from the underlying repository
- Inspecting cumulative cache statistics

## Run

From this example directory:

```bash
go run .
```

Or from the repository root:

```bash
go run ./examples/basic
```

## Example output

```text
positive cache:
- first lookup: status=hit user={ID:42 Name:Ada}
- direct lookup: user={ID:42 Name:Ada}
- entry has expiration: true
- repository loads: 1

negative cache:
- first lookup: status=negative_hit
- second lookup: status=negative_hit
- repository loads: 2

invalidation:
- lookup after invalidation: status=hit user={ID:42 Name:Ada}
- repository loads: 3

stats:
- entries=2 hits=2 negative_hits=1 misses=3
- loads_found=2 loads_not_found=1 load_errors=0
- invalidated_keys=1 evictions=0 expirations=0
```

The first lookup for user `42` loads the value from the repository and caches it. Subsequent `Get` and `GetEntry`
calls read the cached entry without invoking the repository again.

The first lookup for missing user `404` stores a negative cache entry, so the second `GetOrLoad` returns
`LookupNegativeHit` without another repository call.

After `Invalidate(42)`, the next `GetOrLoad` reloads the user from the repository and publishes a new cache entry.