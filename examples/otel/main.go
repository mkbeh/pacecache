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
	ID   string
	Name string
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	// 1. Configure an OpenTelemetry metrics exporter.
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

	// 2. Create the pacecache OpenTelemetry integration.
	metrics := paceotel.New(
		paceotel.WithMeterProvider(meterProvider),
	)

	users, err := pacecache.New[user](
		"users",
		pacecache.WithTTL(time.Minute),
		pacecache.WithNegativeTTL(10*time.Second),
		pacecache.WithMetrics(metrics),
	)
	if err != nil {
		return fmt.Errorf("create users cache: %w", err)
	}
	defer users.Close()

	loadUser := func(ctx context.Context) (user, bool, error) {
		return user{
			ID:   "42",
			Name: "Ada",
		}, true, nil
	}

	// 3. Generate cache activity.
	_, _, err = users.GetOrLoad(
		ctx,
		"42",
		loadUser,
	)
	if err != nil {
		return fmt.Errorf("load user: %w", err)
	}

	_, _, err = users.GetOrLoad(
		ctx,
		"42",
		loadUser,
	)
	if err != nil {
		return fmt.Errorf("load cached user: %w", err)
	}

	users.Invalidate("42")

	_, _, err = users.GetOrLoad(
		ctx,
		"42",
		loadUser,
	)
	if err != nil {
		return fmt.Errorf("reload user: %w", err)
	}

	// 4. Collect and export the current cache metrics.
	if err := meterProvider.ForceFlush(ctx); err != nil {
		return fmt.Errorf("flush metrics: %w", err)
	}

	return nil
}
