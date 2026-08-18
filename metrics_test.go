package pacecache

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

type metricsProbe struct {
	provider     StatsProvider
	registration MetricsRegistration
	err          error
	calls        atomic.Int64
}

func (metrics *metricsProbe) RegisterCache(provider StatsProvider) (MetricsRegistration, error) {
	metrics.calls.Add(1)
	metrics.provider = provider

	return metrics.registration, metrics.err
}

type registrationProbe struct {
	closes atomic.Int64
}

func (registration *registrationProbe) Close() {
	registration.closes.Add(1)
}

func TestMetricsRegistrationLifecycle(t *testing.T) {
	registration := &registrationProbe{}
	metrics := &metricsProbe{registration: registration}

	cache, err := New[string, int](
		"users",
		WithMaxEntries(32),
		WithSegmentCount(1),
		WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if got := metrics.calls.Load(); got != 1 {
		t.Fatalf("RegisterCache calls = %d, want 1", got)
	}
	if metrics.provider == nil {
		t.Fatal("RegisterCache provider = nil")
	}
	if got := metrics.provider.Name(); got != "users" {
		t.Fatalf("provider.Name() = %q, want %q", got, "users")
	}

	cache.Set("key", 42, NoExpiration)
	stats := metrics.provider.Stats()
	if stats.EntryCount != 1 {
		t.Fatalf("provider.Stats().EntryCount = %d, want 1", stats.EntryCount)
	}

	cache.Close()
	cache.Close()

	if got := registration.closes.Load(); got != 1 {
		t.Fatalf("registration Close calls = %d, want 1", got)
	}

	if value, found := cache.Get("key"); value != 42 || !found {
		t.Fatalf("Get() after Close = (%d, %t), want (42, true)", value, found)
	}
}

func TestMetricsRegistrationError(t *testing.T) {
	sentinel := errors.New("registration failed")
	metrics := &metricsProbe{err: sentinel}

	cache, err := New[string, int]("users", WithMetrics(metrics))
	if cache != nil {
		t.Fatalf("New() cache = %v, want nil", cache)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("New() error = %v, want wrapped sentinel", err)
	}
	if err == nil || !strings.Contains(err.Error(), "pacecache: register metrics") {
		t.Fatalf("New() error = %v, want register metrics context", err)
	}
	if got := metrics.calls.Load(); got != 1 {
		t.Fatalf("RegisterCache calls = %d, want 1", got)
	}
}

func TestMetricsMayReturnNilRegistration(t *testing.T) {
	metrics := &metricsProbe{}

	cache, err := New[string, int]("users", WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cache.Close()
	cache.Close()
}

func TestRegisterMetricsNilIsNoop(t *testing.T) {
	cache, err := New[string, int]("users")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := cache.registerMetrics(nil); err != nil {
		t.Fatalf("registerMetrics(nil) error = %v", err)
	}
	if cache.metrics != nil {
		t.Fatalf("cache.metrics = %T, want nil", cache.metrics)
	}
}

func TestCacheStatsProvider(t *testing.T) {
	cache, err := New[string, int](
		"users",
		WithMaxEntries(32),
		WithSegmentCount(1),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	provider := cacheStatsProvider[string, int]{cache: cache}
	if got := provider.Name(); got != "users" {
		t.Fatalf("Name() = %q, want %q", got, "users")
	}

	cache.Set("key", 1, NoExpiration)
	if got := provider.Stats().EntryCount; got != 1 {
		t.Fatalf("Stats().EntryCount = %d, want 1", got)
	}
}
