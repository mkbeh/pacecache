# Basic cache

This example demonstrates a typical cache-aside workflow with `pacecache` and an in-memory user repository.

## Key Concepts Covered

- Configuring a bounded generic cache with TTL and jitter
- Loading missing values with `GetOrLoad`
- Reading cached values and expiration metadata with `Get` and `GetEntry`
- Handling missing loader results without caching them
- Invalidating cached values and reloading them from the repository
- Inspecting cache state and cumulative statistics

## Run

Run the example from this directory:

```bash
go run .
```

Or from the repository root:

```bash
go run ./examples/basic
```

## Example Output

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
````

* **Cache-Aside Loading:** The first lookup for user `42` loads the value from the repository and stores it in the
  cache. Subsequent `Get` and `GetEntry` operations return the cached value without calling the repository again, so the
  repository load count remains at `1`.
* **Missing Results:** Missing results are not cached. Looking up user `404` twice therefore invokes the repository
  twice and produces two `not_found` loader outcomes.
* **Invalidation:** After `Invalidate("42")`, the next lookup loads the value from the repository again and stores the
  fresh result in the cache, increasing the repository load count to `4`.