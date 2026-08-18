package pacecache

import (
	"errors"
	"testing"
	"time"
)

type optionsMetrics struct{}

func (optionsMetrics) RegisterCache(StatsProvider) (MetricsRegistration, error) {
	return nil, nil
}

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
	if settings.jitter != 0 {
		t.Fatalf("jitter = %v, want 0", settings.jitter)
	}
	if settings.slidingExpiration {
		t.Fatal("slidingExpiration = true, want false")
	}
	if settings.cleanupInterval != 0 {
		t.Fatalf("cleanupInterval = %v, want 0", settings.cleanupInterval)
	}
	if settings.cleanupBatchSize != defaultCleanupBatchSize {
		t.Fatalf("cleanupBatchSize = %d, want %d", settings.cleanupBatchSize, defaultCleanupBatchSize)
	}
	if settings.cleanupEntryBudget != defaultCleanupEntryBudget {
		t.Fatalf("cleanupEntryBudget = %d, want %d", settings.cleanupEntryBudget, defaultCleanupEntryBudget)
	}
	if settings.metrics != nil {
		t.Fatalf("metrics = %T, want nil", settings.metrics)
	}
}

func TestNewCacheSettingsAppliesOptions(t *testing.T) {
	metrics := optionsMetrics{}

	settings, err := newCacheSettings(
		"users",
		WithMaxEntries(256),
		WithSegmentCount(16),
		WithTTL(2*time.Minute),
		WithJitter(5*time.Second),
		WithSlidingExpiration(),
		WithCleanupInterval(time.Second),
		WithCleanupBatchSize(64),
		WithCleanupEntryBudget(512),
		WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("newCacheSettings() error = %v", err)
	}

	if settings.name != "users" {
		t.Fatalf("name = %q, want %q", settings.name, "users")
	}
	if settings.maxEntries != 256 {
		t.Fatalf("maxEntries = %d, want 256", settings.maxEntries)
	}
	if settings.segmentCount != 16 {
		t.Fatalf("segmentCount = %d, want 16", settings.segmentCount)
	}
	if settings.ttl != 2*time.Minute {
		t.Fatalf("ttl = %v, want %v", settings.ttl, 2*time.Minute)
	}
	if settings.jitter != 5*time.Second {
		t.Fatalf("jitter = %v, want %v", settings.jitter, 5*time.Second)
	}
	if !settings.slidingExpiration {
		t.Fatal("slidingExpiration = false, want true")
	}
	if settings.cleanupInterval != time.Second {
		t.Fatalf("cleanupInterval = %v, want %v", settings.cleanupInterval, time.Second)
	}
	if settings.cleanupBatchSize != 64 {
		t.Fatalf("cleanupBatchSize = %d, want 64", settings.cleanupBatchSize)
	}
	if settings.cleanupEntryBudget != 512 {
		t.Fatalf("cleanupEntryBudget = %d, want 512", settings.cleanupEntryBudget)
	}
	if settings.metrics != metrics {
		t.Fatalf("metrics = %T, want %T", settings.metrics, metrics)
	}
}

func TestNewCacheSettingsRejectsInvalidOptions(t *testing.T) {
	sentinel := errors.New("sentinel")

	tests := []struct {
		name    string
		cache   string
		options []Option
		want    string
	}{
		{
			name:  "empty name",
			cache: "",
			want:  "invalid configuration: cache name must not be blank",
		},
		{
			name:    "nil option",
			cache:   "cache",
			options: []Option{nil},
			want:    "option 0 is nil",
		},
		{
			name:  "option error",
			cache: "cache",
			options: []Option{
				func(*cacheSettings) error { return sentinel },
			},
			want: "apply option 0: sentinel",
		},
		{
			name:    "max entries zero",
			cache:   "cache",
			options: []Option{WithMaxEntries(0)},
			want:    "apply option 0: max entries must be positive",
		},
		{
			name:    "segment count zero",
			cache:   "cache",
			options: []Option{WithSegmentCount(0)},
			want:    "apply option 0: segment count must be positive",
		},
		{
			name:    "ttl zero",
			cache:   "cache",
			options: []Option{WithTTL(0)},
			want:    "apply option 0: ttl must be positive or NoExpiration",
		},
		{
			name:    "ttl invalid negative",
			cache:   "cache",
			options: []Option{WithTTL(-2)},
			want:    "apply option 0: ttl must be positive or NoExpiration",
		},
		{
			name:    "negative jitter",
			cache:   "cache",
			options: []Option{WithJitter(-1)},
			want:    "apply option 0: jitter must not be negative",
		},
		{
			name:    "cleanup interval zero",
			cache:   "cache",
			options: []Option{WithCleanupInterval(0)},
			want:    "apply option 0: cleanup interval must be positive",
		},
		{
			name:    "cleanup batch zero",
			cache:   "cache",
			options: []Option{WithCleanupBatchSize(0)},
			want:    "apply option 0: cleanup batch size must be positive",
		},
		{
			name:    "cleanup budget zero",
			cache:   "cache",
			options: []Option{WithCleanupEntryBudget(0)},
			want:    "apply option 0: cleanup entry budget must be positive",
		},
		{
			name:  "segments exceed capacity",
			cache: "cache",
			options: []Option{
				WithMaxEntries(16),
				WithSegmentCount(17),
			},
			want: "invalid configuration: segment count must not exceed max entries",
		},
		{
			name:  "ttl and jitter overflow",
			cache: "cache",
			options: []Option{
				WithTTL(maxDuration),
				WithJitter(time.Nanosecond),
			},
			want: "invalid configuration: ttl plus jitter exceeds maximum duration",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newCacheSettings(test.cache, test.options...)
			if err == nil {
				t.Fatal("newCacheSettings() error = nil")
			}
			if err.Error() != test.want {
				t.Fatalf("error = %q, want %q", err, test.want)
			}
		})
	}
}

func TestCacheSettingsAcceptBoundaryValues(t *testing.T) {
	settings, err := newCacheSettings(
		"cache",
		WithMaxEntries(defaultStorageSegmentCount),
		WithSegmentCount(defaultStorageSegmentCount),
		WithTTL(NoExpiration),
		WithJitter(maxDuration),
		WithCleanupBatchSize(1),
		WithCleanupEntryBudget(1),
		WithMetrics(nil),
	)
	if err != nil {
		t.Fatalf("newCacheSettings() error = %v", err)
	}

	if settings.ttl != NoExpiration {
		t.Fatalf("ttl = %v, want NoExpiration", settings.ttl)
	}
	if settings.jitter != maxDuration {
		t.Fatalf("jitter = %v, want maxDuration", settings.jitter)
	}
	if settings.metrics != nil {
		t.Fatalf("metrics = %T, want nil", settings.metrics)
	}
}
