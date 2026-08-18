# Basic cache

This example demonstrates a typical cache-aside workflow with `pacecache` and an in-memory user repository.

**It demonstrates:**

- Configuring a bounded generic cache with TTL and jitter
- Loading missing values through `GetOrLoad`
- Reading cached values and entry metadata with `Get` and `GetEntry`
- Returning not-found loader results without storing them
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

The example is a standalone Go module connected to the repository through `go.work`, so it uses the local `pacecache` checkout.

## Example output

```text
cache:
- first lookup: found=true user={ID:42 Name:Ada}
- direct lookup: user={ID:42 Name:Ada}
- entry has expiration: true
- repository loads: 1

not found:
- first lookup: found=false
- second lookup: found=false
- repository loads: 3

invalidation:
- lookup after invalidation: found=true user={ID:42 Name:Ada}
- repository loads: 4

stats:
- entries=1 hits=2 misses=4
- loads_found=2 loads_not_found=2 load_errors=0
- invalidated_keys=1 evictions=0 expirations=0
```

The first lookup for user `42` loads the value from the repository and caches it. Subsequent `Get` and `GetEntry`
calls read the cached entry without invoking the repository again.

User `404` does not exist. Not-found loader results are not stored, so both `GetOrLoad` calls reach the repository.

After `Invalidate(42)`, the next `GetOrLoad` reloads the user from the repository and publishes a new cache entry.
