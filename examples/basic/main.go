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

	loadUser := func(id int64) (user, bool, error) {
		return users.GetOrLoad(
			ctx,
			id,
			func(ctx context.Context) (user, bool, error) {
				return repository.find(ctx, id)
			},
		)
	}

	// The first lookup loads the user from the underlying repository.
	first, found, err := loadUser(42)
	if err != nil {
		return fmt.Errorf("load user: %w", err)
	}

	fmt.Println("positive cache:")
	fmt.Printf("- first lookup: found=%t user=%+v\n", found, first)

	// The second lookup is served from the cache.
	second, found, err := loadUser(42)
	if err != nil {
		return fmt.Errorf("load cached user: %w", err)
	}

	fmt.Printf("- second lookup: found=%t user=%+v\n", found, second)
	fmt.Printf("- repository loads: %d\n", repository.loads)

	// Not-found results are cached using the configured negative TTL.
	_, found, err = loadUser(404)
	if err != nil {
		return fmt.Errorf("load missing user: %w", err)
	}

	fmt.Println()
	fmt.Println("negative cache:")
	fmt.Printf("- first lookup: found=%t\n", found)

	_, found, err = loadUser(404)
	if err != nil {
		return fmt.Errorf("load cached missing user: %w", err)
	}

	fmt.Printf("- second lookup: found=%t\n", found)
	fmt.Printf("- repository loads: %d\n", repository.loads)

	// Explicit invalidation forces the next lookup to reload the value.
	users.Invalidate(42)

	reloaded, found, err := loadUser(42)
	if err != nil {
		return fmt.Errorf("reload invalidated user: %w", err)
	}

	fmt.Println()
	fmt.Println("invalidation:")
	fmt.Printf("- lookup after invalidation: found=%t user=%+v\n", found, reloaded)
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
