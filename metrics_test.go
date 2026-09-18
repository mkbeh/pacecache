package pacecache

import (
	"errors"
	"slices"
	"sync"
	"testing"
)

type testMetrics struct {
	mu sync.Mutex

	registerCalls int
	sources       []MetricsSource
	err           error
}

func (metrics *testMetrics) Register(source MetricsSource) error {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	metrics.registerCalls++

	if metrics.err != nil {
		return metrics.err
	}

	metrics.sources = append(metrics.sources, source)

	return nil
}

func (metrics *testMetrics) snapshot() (int, []MetricsSource) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	return metrics.registerCalls, slices.Clone(metrics.sources)
}

func TestMetricsRegistersSource(t *testing.T) {
	metrics := &testMetrics{}

	cache, err := New[string, int](
		WithName("users"),
		WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	registerCalls, sources := metrics.snapshot()
	if registerCalls != 1 {
		t.Fatalf("Register() calls = %d, want 1", registerCalls)
	}
	if len(sources) != 1 {
		t.Fatalf("registered sources = %d, want 1", len(sources))
	}

	source := sources[0]
	if source.Name() != "users" {
		t.Fatalf("source name = %q, want users", source.Name())
	}
	if _, ok := source.(interface{ Close() }); ok {
		t.Fatal("metrics source unexpectedly exposes Cache.Close")
	}

	cache.Set("a", 1, NoExpiration)
	if got := source.Stats().EntryCount; got != 1 {
		t.Fatalf("source Stats().EntryCount = %d, want 1", got)
	}
}

func TestMetricsRegistersMultipleCaches(t *testing.T) {
	metrics := &testMetrics{}

	users, err := New[string, int](
		WithName("users"),
		WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("New(users) error = %v", err)
	}

	sessions, err := New[string, int](
		WithName("sessions"),
		WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("New(sessions) error = %v", err)
	}

	users.Set("a", 1, NoExpiration)
	sessions.Set("a", 1, NoExpiration)
	sessions.Set("b", 2, NoExpiration)

	registerCalls, sources := metrics.snapshot()
	if registerCalls != 2 {
		t.Fatalf("Register() calls = %d, want 2", registerCalls)
	}
	if len(sources) != 2 {
		t.Fatalf("registered sources = %d, want 2", len(sources))
	}

	byName := make(map[string]MetricsSource, len(sources))
	for _, source := range sources {
		if _, ok := source.(interface{ Close() }); ok {
			t.Fatalf("metrics source %q unexpectedly exposes Cache.Close", source.Name())
		}

		if _, exists := byName[source.Name()]; exists {
			t.Fatalf("metrics source %q registered more than once", source.Name())
		}

		byName[source.Name()] = source
	}

	usersSource, ok := byName["users"]
	if !ok {
		t.Fatal("users metrics source not registered")
	}
	if got := usersSource.Stats().EntryCount; got != 1 {
		t.Fatalf("users Stats().EntryCount = %d, want 1", got)
	}

	sessionsSource, ok := byName["sessions"]
	if !ok {
		t.Fatal("sessions metrics source not registered")
	}
	if got := sessionsSource.Stats().EntryCount; got != 2 {
		t.Fatalf("sessions Stats().EntryCount = %d, want 2", got)
	}
}

func TestMetricsRegistersUnnamedSource(t *testing.T) {
	metrics := &testMetrics{}

	if _, err := New[string, int](WithMetrics(metrics)); err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, sources := metrics.snapshot()
	if len(sources) != 1 {
		t.Fatalf("registered sources = %d, want 1", len(sources))
	}
	if sources[0].Name() != "" {
		t.Fatalf("source name = %q, want empty", sources[0].Name())
	}
}

func TestMetricsRegisterError(t *testing.T) {
	sentinel := errors.New("register failed")
	metrics := &testMetrics{err: sentinel}

	cache, err := New[string, int](
		WithName("users"),
		WithMetrics(metrics),
	)
	if cache != nil {
		t.Fatal("cache must be nil when metrics registration fails")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want wrapped sentinel", err)
	}

	registerCalls, sources := metrics.snapshot()
	if registerCalls != 1 {
		t.Fatalf("Register() calls = %d, want 1", registerCalls)
	}
	if len(sources) != 0 {
		t.Fatalf("registered sources = %d, want 0", len(sources))
	}
}

func TestMetricsNilIsNoop(t *testing.T) {
	cache, err := New[string, int](WithMetrics(nil))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cache.Set("a", 1, NoExpiration)

	value, found := cache.Get("a")
	if !found || value != 1 {
		t.Fatalf("Get(a) = %d, %t, want 1, true", value, found)
	}
}
