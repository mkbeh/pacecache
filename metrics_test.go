package pacecache

import (
	"errors"
	"sync/atomic"
	"testing"
)

type testMetrics struct {
	registerCalls atomic.Int64
	provider      StatsProvider
	registration  *testMetricsRegistration
	err           error
}

func (metrics *testMetrics) RegisterCache(provider StatsProvider) (MetricsRegistration, error) {
	metrics.registerCalls.Add(1)
	metrics.provider = provider

	if metrics.err != nil {
		return nil, metrics.err
	}

	if metrics.registration == nil {
		metrics.registration = &testMetricsRegistration{}
	}

	return metrics.registration, nil
}

type testMetricsRegistration struct {
	closeCalls atomic.Int64
}

func (registration *testMetricsRegistration) Close() {
	registration.closeCalls.Add(1)
}

func TestMetricsRegistrationLifecycle(t *testing.T) {
	metrics := &testMetrics{}

	cache, err := New[int]("users", WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if metrics.registerCalls.Load() != 1 {
		t.Fatalf("RegisterCache calls = %d, want 1", metrics.registerCalls.Load())
	}
	if metrics.provider != cache {
		t.Fatal("RegisterCache provider is not the initialized cache")
	}
	if metrics.provider.Name() != "users" {
		t.Fatalf("provider.Name() = %q, want %q", metrics.provider.Name(), "users")
	}

	cache.Close()
	cache.Close()

	if metrics.registration.closeCalls.Load() != 1 {
		t.Fatalf("registration Close calls = %d, want 1", metrics.registration.closeCalls.Load())
	}
}

func TestNewPropagatesMetricsRegistrationError(t *testing.T) {
	sentinel := errors.New("register failed")
	metrics := &testMetrics{err: sentinel}

	cache, err := New[int]("users", WithMetrics(metrics))
	if err == nil {
		t.Fatal("New() error = nil")
	}
	if cache != nil {
		t.Fatal("New() cache is not nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("New() error = %v, want errors.Is(..., sentinel)", err)
	}
}

func TestNilCacheLifecycleMethods(t *testing.T) {
	var cache *Cache[int]

	if cache.Name() != "" {
		t.Fatalf("Name() = %q, want empty", cache.Name())
	}
	if got := cache.Stats(); got != (Stats{}) {
		t.Fatalf("Stats() = %+v, want zero value", got)
	}

	cache.Close()
	cache.Invalidate("key")
	cache.InvalidateAll()
}
