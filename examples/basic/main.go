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

	users, err := pacecache.NewWithLoader[int64, user](
		"users",
		repository.find,
		pacecache.WithMaxEntries(128),
		pacecache.WithTTL(30*time.Second),
		pacecache.WithJitter(5*time.Second),
	)
	if err != nil {
		return fmt.Errorf("create users cache: %w", err)
	}
	defer users.Close()

	// The first lookup loads the user from the underlying repository.
	first, found, err := users.GetOrLoad(ctx, 42)
	if err != nil {
		return fmt.Errorf("load user: %w", err)
	}
	if !found {
		return fmt.Errorf("user 42 was not found")
	}

	fmt.Println("cache:")
	fmt.Printf("- first lookup: found=%t user=%+v\n", found, first)

	// A direct lookup reads the already cached value without invoking the loader.
	cached, found := users.Get(42)
	if !found {
		return fmt.Errorf("cached user 42 was not found")
	}

	fmt.Printf("- direct lookup: user=%+v\n", cached)

	// GetEntry exposes cache metadata without changing the value lookup semantics.
	entry, found := users.GetEntry(42)
	if !found {
		return fmt.Errorf("cached entry 42 was not found")
	}

	fmt.Printf("- entry has expiration: %t\n", !entry.ExpiresAt().IsZero())
	fmt.Printf("- repository loads: %d\n", repository.loads)

	// Not-found loader results are returned but are not stored by pacecache.
	_, found, err = users.GetOrLoad(ctx, 404)
	if err != nil {
		return fmt.Errorf("load missing user: %w", err)
	}
	if found {
		return fmt.Errorf("user 404 unexpectedly exists")
	}

	fmt.Println()
	fmt.Println("not found:")
	fmt.Printf("- first lookup: found=%t\n", found)

	_, found, err = users.GetOrLoad(ctx, 404)
	if err != nil {
		return fmt.Errorf("load missing user again: %w", err)
	}
	if found {
		return fmt.Errorf("user 404 unexpectedly exists")
	}

	fmt.Printf("- second lookup: found=%t\n", found)
	fmt.Printf("- repository loads: %d\n", repository.loads)

	// Explicit deletion forces the next lookup to reload the value.
	users.Delete(42)

	reloaded, found, err := users.GetOrLoad(ctx, 42)
	if err != nil {
		return fmt.Errorf("reload deleted user: %w", err)
	}
	if !found {
		return fmt.Errorf("reloaded user 42 was not found")
	}

	fmt.Println()
	fmt.Println("deletion:")
	fmt.Printf("- lookup after deletion: found=%t user=%+v\n", found, reloaded)
	fmt.Printf("- repository loads: %d\n", repository.loads)

	stats := users.Stats()

	fmt.Println()
	fmt.Println("stats:")
	fmt.Printf(
		"- entries=%d hits=%d misses=%d\n",
		stats.EntryCount,
		stats.HitCount,
		stats.MissCount,
	)
	fmt.Printf(
		"- loads_found=%d loads_not_found=%d load_errors=%d\n",
		stats.LoadFoundCount,
		stats.LoadNotFoundCount,
		stats.LoadErrorCount,
	)
	fmt.Printf(
		"- deleted_entries=%d evictions=%d expirations=%d\n",
		stats.DeletedEntryCount,
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
