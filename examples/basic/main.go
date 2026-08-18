package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mkbeh/pacecache"
)

type user struct {
	ID   int64
	Name string
}

type userRepository struct {
	users map[int64]user
	loads int
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	repository := &userRepository{
		users: map[int64]user{
			42: {
				ID:   42,
				Name: "Ada",
			},
		},
	}

	users, err := pacecache.New[int64, user](
		"users",
		pacecache.WithMaxEntries(128),
		pacecache.WithTTL(30*time.Second),
		pacecache.WithJitter(5*time.Second),
		pacecache.WithNegativeTTL(5*time.Second),
	)
	if err != nil {
		return fmt.Errorf("create users cache: %w", err)
	}
	defer users.Close()

	loadUser := func(id int64) (user, pacecache.LookupStatus, error) {
		return users.GetOrLoad(
			ctx,
			id,
			func(ctx context.Context) (user, bool, error) {
				return repository.find(ctx, id)
			},
		)
	}

	// The first lookup loads the user from the underlying repository.
	first, status, err := loadUser(42)
	if err != nil {
		return fmt.Errorf("load user: %w", err)
	}
	if status != pacecache.LookupHit {
		return fmt.Errorf("load user 42: status=%d", status)
	}

	fmt.Println("positive cache:")
	fmt.Printf("- first lookup: status=hit user=%+v\n", first)

	// A direct lookup reads the already cached value without invoking the loader.
	cached, status := users.Get(42)
	if status != pacecache.LookupHit {
		return fmt.Errorf("get cached user: status=%d", status)
	}

	fmt.Printf("- direct lookup: user=%+v\n", cached)

	// GetEntry exposes cache metadata without changing the value lookup semantics.
	entry, status := users.GetEntry(42)
	if status != pacecache.LookupHit {
		return fmt.Errorf("get cached entry: status=%d", status)
	}

	fmt.Printf("- entry has expiration: %t\n", !entry.ExpiresAt().IsZero())
	fmt.Printf("- repository loads: %d\n", repository.loads)

	// Not-found results are cached using the configured negative TTL.
	_, status, err = loadUser(404)
	if err != nil {
		return fmt.Errorf("load missing user: %w", err)
	}
	if status != pacecache.LookupNegativeHit {
		return fmt.Errorf("load user 404: status=%d", status)
	}

	fmt.Println()
	fmt.Println("negative cache:")
	fmt.Println("- first lookup: status=negative_hit")

	_, status, err = loadUser(404)
	if err != nil {
		return fmt.Errorf("load cached missing user: %w", err)
	}
	if status != pacecache.LookupNegativeHit {
		return fmt.Errorf("load cached user 404: status=%d", status)
	}

	fmt.Println("- second lookup: status=negative_hit")
	fmt.Printf("- repository loads: %d\n", repository.loads)

	// Explicit invalidation forces the next lookup to reload the value.
	users.Invalidate(42)

	reloaded, status, err := loadUser(42)
	if err != nil {
		return fmt.Errorf("reload invalidated user: %w", err)
	}
	if status != pacecache.LookupHit {
		return fmt.Errorf("reload user 42: status=%d", status)
	}

	fmt.Println()
	fmt.Println("invalidation:")
	fmt.Printf("- lookup after invalidation: status=hit user=%+v\n", reloaded)
	fmt.Printf("- repository loads: %d\n", repository.loads)

	stats := users.Stats()

	fmt.Println()
	fmt.Println("stats:")
	fmt.Printf(
		"- entries=%d hits=%d negative_hits=%d misses=%d\n",
		stats.EntryCount,
		stats.HitCount,
		stats.NegativeHitCount,
		stats.MissCount,
	)
	fmt.Printf(
		"- loads_found=%d loads_not_found=%d load_errors=%d\n",
		stats.LoadFoundCount,
		stats.LoadNotFoundCount,
		stats.LoadErrorCount,
	)
	fmt.Printf(
		"- invalidated_keys=%d evictions=%d expirations=%d\n",
		stats.InvalidatedKeyCount,
		stats.EvictionCount,
		stats.ExpirationCount,
	)

	return nil
}

func (repository *userRepository) find(ctx context.Context, id int64) (user, bool, error) {
	if err := ctx.Err(); err != nil {
		return user{}, false, err
	}

	repository.loads++

	value, found := repository.users[id]

	return value, found, nil
}
