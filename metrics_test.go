package pacecache

import (
	"errors"
	"sync/atomic"
	"testing"
)

type testMetrics struct {
	registerCalls atomic.Int64
	provider      StatsProvider
	registration  MetricsRegistration
	err           error
	providerClose bool
}

func (metrics *testMetrics) RegisterCache(provider StatsProvider) (MetricsRegistration, error) {
	metrics.registerCalls.Add(1)
	metrics.provider = provider
	_, metrics.providerClose = provider.(interface{ Close() })
	return metrics.registration, metrics.err
}

type testMetricsRegistration struct {
	closeCalls atomic.Int64
}

func (registration *testMetricsRegistration) Close() {
	registration.closeCalls.Add(1)
}

func TestMetricsRegistrationLifecycle(t *testing.T) {
	registration := &testMetricsRegistration{}
	metrics := &testMetrics{registration: registration}

	cache, err := New[string, int]("users", WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if metrics.registerCalls.Load() != 1 {
		t.Fatalf("RegisterCache calls = %d, want 1", metrics.registerCalls.Load())
	}
	if metrics.provider == nil {
		t.Fatal("metrics provider is nil")
	}
	if metrics.provider.Name() != "users" {
		t.Fatalf("provider name = %q, want users", metrics.provider.Name())
	}
	if metrics.providerClose {
		t.Fatal("metrics provider unexpectedly exposes Cache.Close")
	}

	cache.Set("a", 1, NoExpiration)
	if got := metrics.provider.Stats().EntryCount; got != 1 {
		t.Fatalf("provider Stats().EntryCount = %d, want 1", got)
	}

	cache.Close()
	cache.Close()
	if registration.closeCalls.Load() != 1 {
		t.Fatalf("registration Close calls = %d, want 1", registration.closeCalls.Load())
	}
}

func TestMetricsRegistrationError(t *testing.T) {
	sentinel := errors.New("register failed")
	metrics := &testMetrics{err: sentinel}

	cache, err := New[string, int]("users", WithMetrics(metrics))
	if cache != nil {
		t.Fatal("cache must be nil when metrics registration fails")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want wrapped sentinel", err)
	}
	if metrics.registerCalls.Load() != 1 {
		t.Fatalf("RegisterCache calls = %d, want 1", metrics.registerCalls.Load())
	}
}

func TestMetricsMayReturnNilRegistration(t *testing.T) {
	metrics := &testMetrics{}
	cache, err := New[string, int]("users", WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cache.Close()
}

func TestRegisterMetricsNilIsNoop(t *testing.T) {
	cache := &Cache[string, int]{}
	if err := cache.registerMetrics(nil); err != nil {
		t.Fatalf("registerMetrics(nil) error = %v", err)
	}
	if cache.metrics != nil {
		t.Fatal("metrics registration must remain nil")
	}
}
