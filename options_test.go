package pacecache

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDefaultCacheSettings(t *testing.T) {
	settings := defaultCacheSettings()

	if settings.maxEntries != defaultMaxEntries {
		t.Fatalf("maxEntries = %d, want %d", settings.maxEntries, defaultMaxEntries)
	}
	if settings.segmentCount != defaultStorageSegmentCount {
		t.Fatalf("segmentCount = %d, want %d", settings.segmentCount, defaultStorageSegmentCount)
	}
	if settings.ttl != defaultTTL {
		t.Fatalf("ttl = %v, want %v", settings.ttl, defaultTTL)
	}
	if settings.jitter != 0 || settings.cleanupInterval != 0 {
		t.Fatalf("optional durations must be disabled by default: %+v", settings)
	}
	if settings.cleanupBatchSize != defaultCleanupBatchSize {
		t.Fatalf("cleanupBatchSize = %d, want %d", settings.cleanupBatchSize, defaultCleanupBatchSize)
	}
	if settings.cleanupEntryBudget != defaultCleanupEntryBudget {
		t.Fatalf("cleanupEntryBudget = %d, want %d", settings.cleanupEntryBudget, defaultCleanupEntryBudget)
	}
	if settings.slidingExpiration {
		t.Fatal("sliding expiration must be disabled by default")
	}
	if settings.metrics != nil {
		t.Fatal("metrics must be nil by default")
	}
}

func TestNewCacheSettingsAppliesOptions(t *testing.T) {
	metrics := &testMetrics{}

	settings, err := newCacheSettings(
		"users",
		WithMaxEntries(128),
		WithSegmentCount(8),
		WithTTL(2*time.Minute),
		WithJitter(15*time.Second),
		WithSlidingExpiration(),
		WithCleanupInterval(time.Second),
		WithCleanupBatchSize(1024),
		WithCleanupEntryBudget(64*1024),
		WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("newCacheSettings() error = %v", err)
	}

	if settings.name != "users" ||
		settings.maxEntries != 128 ||
		settings.segmentCount != 8 ||
		settings.ttl != 2*time.Minute ||
		settings.jitter != 15*time.Second ||
		!settings.slidingExpiration ||
		settings.cleanupInterval != time.Second ||
		settings.cleanupBatchSize != 1024 ||
		settings.cleanupEntryBudget != 64*1024 ||
		settings.metrics != metrics {
		t.Fatalf("unexpected settings: %+v", settings)
	}
}

func TestNewCacheSettingsRejectsNilOption(t *testing.T) {
	_, err := newCacheSettings("users", nil)
	if err == nil || !strings.Contains(err.Error(), "option 0 is nil") {
		t.Fatalf("error = %v, want nil-option error", err)
	}
}

func TestNewCacheSettingsWrapsOptionError(t *testing.T) {
	sentinel := errors.New("sentinel")
	option := func(*cacheSettings) error { return sentinel }

	_, err := newCacheSettings("users", option)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want wrapped sentinel", err)
	}
}

func TestCacheSettingsValidation(t *testing.T) {
	tests := []struct {
		name    string
		cache   string
		options []Option
		want    string
	}{
		{name: "blank name", cache: "", want: "cache name must not be blank"},
		{name: "max entries zero", cache: "users", options: []Option{WithMaxEntries(0)}, want: "max entries must be positive"},
		{name: "max entries negative", cache: "users", options: []Option{WithMaxEntries(-1)}, want: "max entries must be positive"},
		{name: "segment count zero", cache: "users", options: []Option{WithSegmentCount(0)}, want: "segment count must be positive"},
		{name: "segment count negative", cache: "users", options: []Option{WithSegmentCount(-1)}, want: "segment count must be positive"},
		{name: "ttl zero", cache: "users", options: []Option{WithTTL(0)}, want: "ttl must be positive or NoExpiration"},
		{name: "ttl invalid negative", cache: "users", options: []Option{WithTTL(-2)}, want: "ttl must be positive or NoExpiration"},
		{name: "negative jitter", cache: "users", options: []Option{WithJitter(-1)}, want: "jitter must not be negative"},
		{name: "cleanup interval zero", cache: "users", options: []Option{WithCleanupInterval(0)}, want: "cleanup interval must be positive"},
		{name: "cleanup interval negative", cache: "users", options: []Option{WithCleanupInterval(-1)}, want: "cleanup interval must be positive"},
		{name: "cleanup batch size zero", cache: "users", options: []Option{WithCleanupBatchSize(0)}, want: "cleanup batch size must be positive"},
		{name: "cleanup batch size negative", cache: "users", options: []Option{WithCleanupBatchSize(-1)}, want: "cleanup batch size must be positive"},
		{name: "cleanup entry budget zero", cache: "users", options: []Option{WithCleanupEntryBudget(0)}, want: "cleanup entry budget must be positive"},
		{name: "cleanup entry budget negative", cache: "users", options: []Option{WithCleanupEntryBudget(-1)}, want: "cleanup entry budget must be positive"},
		{name: "segments exceed max entries", cache: "users", options: []Option{WithMaxEntries(2), WithSegmentCount(3)}, want: "segment count must not exceed max entries"},
		{
			name:  "ttl plus jitter overflow",
			cache: "users",
			options: []Option{
				WithTTL(maxDuration),
				WithJitter(time.Nanosecond),
			},
			want: "ttl plus jitter exceeds maximum duration",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newCacheSettings(test.cache, test.options...)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestCacheSettingsAcceptsIndependentCleanupLimits(t *testing.T) {
	settings, err := newCacheSettings(
		"users",
		WithMaxEntries(4),
		WithSegmentCount(1),
		WithCleanupBatchSize(10_000),
		WithCleanupEntryBudget(3),
	)
	if err != nil {
		t.Fatalf("newCacheSettings() error = %v", err)
	}

	if settings.cleanupBatchSize != 10_000 || settings.cleanupEntryBudget != 3 {
		t.Fatalf("cleanup limits = %d/%d, want 10000/3", settings.cleanupBatchSize, settings.cleanupEntryBudget)
	}
}

func TestCacheSettingsAcceptsBoundaryValues(t *testing.T) {
	settings, err := newCacheSettings(
		"users",
		WithMaxEntries(1),
		WithSegmentCount(1),
		WithTTL(NoExpiration),
		WithJitter(maxDuration),
		WithMetrics(nil),
	)
	if err != nil {
		t.Fatalf("newCacheSettings() error = %v", err)
	}

	if settings.ttl != NoExpiration {
		t.Fatalf("ttl = %v, want NoExpiration", settings.ttl)
	}
	if settings.metrics != nil {
		t.Fatal("metrics must remain nil")
	}
}
