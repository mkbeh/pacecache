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
		pacecache.WithNegativeTTL(10*time.Second),
		pacecache.WithMetrics(metrics),
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

	// 3. Generate representative cache activity.
	lookups := []struct {
		id     int64
		status pacecache.LookupStatus
	}{
		{id: 42, status: pacecache.LookupHit},
		{id: 42, status: pacecache.LookupHit},
		{id: 404, status: pacecache.LookupNegativeHit},
		{id: 404, status: pacecache.LookupNegativeHit},
	}

	for _, lookup := range lookups {
		_, status, err := loadUser(lookup.id)
		if err != nil {
			return fmt.Errorf("load user %d: %w", lookup.id, err)
		}
		if status != lookup.status {
			return fmt.Errorf(
				"load user %d: status=%d, want=%d",
				lookup.id,
				status,
				lookup.status,
			)
		}
	}

	users.Invalidate(42)

	_, status, err := loadUser(42)
	if err != nil {
		return fmt.Errorf("reload invalidated user: %w", err)
	}
	if status != pacecache.LookupHit {
		return fmt.Errorf(
			"reload invalidated user: status=%d, want=%d",
			status,
			pacecache.LookupHit,
		)
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
