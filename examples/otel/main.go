package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mkbeh/pacecache"
	"github.com/mkbeh/pacecache/extra/paceotel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type user struct {
	ID   int64
	Name string
}

type userRepository struct {
	users map[int64]user
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	// 1. Configure an OpenTelemetry SDK exporter and MeterProvider.
	exporter, err := stdoutmetric.New(
		stdoutmetric.WithPrettyPrint(),
	)
	if err != nil {
		return fmt.Errorf("create metrics exporter: %w", err)
	}

	reader := sdkmetric.NewPeriodicReader(exporter)

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
	)
	defer func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			log.Printf("shutdown meter provider: %v", err)
		}
	}()

	// 2. Create one reusable pacecache OpenTelemetry integration.
	metrics := paceotel.New(
		paceotel.WithMeterProvider(meterProvider),
	)

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
		pacecache.WithTTL(time.Minute),
		pacecache.WithMetrics(metrics),
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

	// 3. Generate representative cache activity.
	lookups := []struct {
		id    int64
		found bool
	}{
		{id: 42, found: true},
		{id: 42, found: true},
		{id: 404, found: false},
		{id: 404, found: false},
	}

	for _, lookup := range lookups {
		_, found, err := loadUser(lookup.id)
		if err != nil {
			return fmt.Errorf("load user %d: %w", lookup.id, err)
		}
		if found != lookup.found {
			return fmt.Errorf("load user %d: found=%t", lookup.id, found)
		}
	}

	users.Invalidate(42)

	if _, found, err := loadUser(42); err != nil {
		return fmt.Errorf("reload invalidated user: %w", err)
	} else if !found {
		return fmt.Errorf("reloaded user 42 was not found")
	}

	// 4. Force one collection because this short-lived example exits
	// immediately. Long-running applications normally export periodically.
	if err := meterProvider.ForceFlush(ctx); err != nil {
		return fmt.Errorf("flush metrics: %w", err)
	}

	return nil
}

func (repository *userRepository) find(
	ctx context.Context,
	id int64,
) (user, bool, error) {
	if err := ctx.Err(); err != nil {
		return user{}, false, err
	}

	value, found := repository.users[id]

	return value, found, nil
}
