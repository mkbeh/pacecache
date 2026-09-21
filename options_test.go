package pacecache

import (
	"testing"
	"time"
)

func TestDefaultSettings(t *testing.T) {
	settings := defaultSettings()

	if settings.name != "" {
		t.Fatalf("name = %q, want empty", settings.name)
	}
	if settings.maxEntries != defaultMaxEntries {
		t.Fatalf("maxEntries = %d, want %d", settings.maxEntries, defaultMaxEntries)
	}
	if settings.segmentCount != defaultStorageSegmentCount {
		t.Fatalf(
			"segmentCount = %d, want %d",
			settings.segmentCount,
			defaultStorageSegmentCount,
		)
	}
	if settings.ttl != defaultTTL {
		t.Fatalf("ttl = %v, want %v", settings.ttl, defaultTTL)
	}
	if settings.jitter != 0 {
		t.Fatalf("jitter = %v, want 0", settings.jitter)
	}
	if settings.cleanupInterval != defaultCleanupInterval {
		t.Fatalf(
			"cleanupInterval = %v, want %v",
			settings.cleanupInterval,
			defaultCleanupInterval,
		)
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
}

func TestNewSettingsAppliesOptions(t *testing.T) {
	got := newSettings(
		WithName("users"),
		WithMaxEntries(128),
		WithSegmentCount(8),
		WithTTL(2*time.Minute),
		WithJitter(15*time.Second),
		WithSlidingExpiration(),
		WithCleanupInterval(time.Second),
		WithCleanupBatchSize(1024),
		WithCleanupEntryBudget(64*1024),
	)

	want := settings{
		name:               "users",
		maxEntries:         128,
		segmentCount:       8,
		ttl:                2 * time.Minute,
		jitter:             15 * time.Second,
		slidingExpiration:  true,
		cleanupInterval:    time.Second,
		cleanupBatchSize:   1024,
		cleanupEntryBudget: 64 * 1024,
	}

	if *got != want {
		t.Fatalf("newSettings() = %+v, want %+v", *got, want)
	}
}

func TestNewSettingsIgnoresNilOption(t *testing.T) {
	got := newSettings(nil)
	want := defaultSettings()

	if *got != *want {
		t.Fatalf("newSettings(nil) = %+v, want %+v", *got, *want)
	}
}

func TestNewSettingsIgnoresInvalidOptionValues(t *testing.T) {
	settings := newSettings(
		WithMaxEntries(128),
		WithMaxEntries(0),
		WithSegmentCount(8),
		WithSegmentCount(-1),
		WithTTL(time.Minute),
		WithTTL(0),
		WithJitter(time.Second),
		WithJitter(-1),
		WithCleanupInterval(time.Second),
		WithCleanupInterval(0),
		WithCleanupBatchSize(1024),
		WithCleanupBatchSize(0),
		WithCleanupEntryBudget(4096),
		WithCleanupEntryBudget(-1),
	)

	if settings.maxEntries != 128 {
		t.Fatalf("maxEntries = %d, want 128", settings.maxEntries)
	}
	if settings.segmentCount != 8 {
		t.Fatalf("segmentCount = %d, want 8", settings.segmentCount)
	}
	if settings.ttl != time.Minute {
		t.Fatalf("ttl = %v, want 1m", settings.ttl)
	}
	if settings.jitter != time.Second {
		t.Fatalf("jitter = %v, want 1s", settings.jitter)
	}
	if settings.cleanupInterval != time.Second {
		t.Fatalf("cleanupInterval = %v, want 1s", settings.cleanupInterval)
	}
	if settings.cleanupBatchSize != 1024 {
		t.Fatalf("cleanupBatchSize = %d, want 1024", settings.cleanupBatchSize)
	}
	if settings.cleanupEntryBudget != 4096 {
		t.Fatalf("cleanupEntryBudget = %d, want 4096", settings.cleanupEntryBudget)
	}
}

func TestNewSettingsClampsSegmentCountToMaxEntries(t *testing.T) {
	tests := []struct {
		name    string
		options []Option
	}{
		{
			name: "max entries first",
			options: []Option{
				WithMaxEntries(2),
				WithSegmentCount(3),
			},
		},
		{
			name: "segment count first",
			options: []Option{
				WithSegmentCount(3),
				WithMaxEntries(2),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			settings := newSettings(test.options...)
			if settings.maxEntries != 2 || settings.segmentCount != 2 {
				t.Fatalf(
					"settings = maxEntries:%d segmentCount:%d, want 2/2",
					settings.maxEntries,
					settings.segmentCount,
				)
			}
		})
	}
}

func TestSettingsAcceptsIndependentCleanupLimits(t *testing.T) {
	settings := newSettings(
		WithMaxEntries(4),
		WithSegmentCount(1),
		WithCleanupBatchSize(10_000),
		WithCleanupEntryBudget(3),
	)

	if settings.cleanupBatchSize != 10_000 || settings.cleanupEntryBudget != 3 {
		t.Fatalf("cleanup limits = %d/%d, want 10000/3", settings.cleanupBatchSize, settings.cleanupEntryBudget)
	}
}

func TestSettingsAcceptsBoundaryValues(t *testing.T) {
	settings := newSettings(
		WithName(""),
		WithMaxEntries(1),
		WithTTL(NoExpiration),
		WithJitter(maxDuration),
	)

	if settings.name != "" {
		t.Fatalf("name = %q, want empty", settings.name)
	}
	if settings.ttl != NoExpiration {
		t.Fatalf("ttl = %v, want NoExpiration", settings.ttl)
	}
	if settings.jitter != maxDuration {
		t.Fatalf("jitter = %v, want maxDuration", settings.jitter)
	}
}

func TestNewSettingsAcceptsMaximumTTLAndJitter(t *testing.T) {
	settings := newSettings(
		WithTTL(maxDuration),
		WithJitter(maxDuration),
	)

	if settings.ttl != maxDuration {
		t.Fatalf("ttl = %v, want maxDuration", settings.ttl)
	}
	if settings.jitter != maxDuration {
		t.Fatalf("jitter = %v, want maxDuration", settings.jitter)
	}
}
