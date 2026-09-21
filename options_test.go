package pacecache

import (
	"errors"
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
	if settings.segmentCount != 1 {
		t.Fatalf("segmentCount = %d, want 1", settings.segmentCount)
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
	got, err := newSettings(
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
	if err != nil {
		t.Fatalf("newSettings() error = %v", err)
	}

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

func TestNewSettingsRejectsNilOption(t *testing.T) {
	_, err := newSettings(nil)
	const want = "option 0 is nil"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func TestNewSettingsWrapsOptionError(t *testing.T) {
	sentinel := errors.New("sentinel")
	option := func(*settings) error { return sentinel }

	_, err := newSettings(option)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want wrapped sentinel", err)
	}
}

func TestSettingsValidation(t *testing.T) {
	tests := []struct {
		name    string
		options []Option
		want    string
	}{
		{name: "max entries zero", options: []Option{WithMaxEntries(0)}, want: "apply option 0: max entries must be positive"},
		{name: "max entries negative", options: []Option{WithMaxEntries(-1)}, want: "apply option 0: max entries must be positive"},
		{name: "segment count zero", options: []Option{WithSegmentCount(0)}, want: "apply option 0: segment count must be positive"},
		{name: "segment count negative", options: []Option{WithSegmentCount(-1)}, want: "apply option 0: segment count must be positive"},
		{name: "ttl zero", options: []Option{WithTTL(0)}, want: "apply option 0: ttl must be positive or NoExpiration"},
		{name: "ttl invalid negative", options: []Option{WithTTL(-2)}, want: "apply option 0: ttl must be positive or NoExpiration"},
		{name: "negative jitter", options: []Option{WithJitter(-1)}, want: "apply option 0: jitter must not be negative"},
		{name: "cleanup interval zero", options: []Option{WithCleanupInterval(0)}, want: "apply option 0: cleanup interval must be positive"},
		{name: "cleanup interval negative", options: []Option{WithCleanupInterval(-1)}, want: "apply option 0: cleanup interval must be positive"},
		{name: "cleanup batch size zero", options: []Option{WithCleanupBatchSize(0)}, want: "apply option 0: cleanup batch size must be positive"},
		{name: "cleanup batch size negative", options: []Option{WithCleanupBatchSize(-1)}, want: "apply option 0: cleanup batch size must be positive"},
		{name: "cleanup entry budget zero", options: []Option{WithCleanupEntryBudget(0)}, want: "apply option 0: cleanup entry budget must be positive"},
		{name: "cleanup entry budget negative", options: []Option{WithCleanupEntryBudget(-1)}, want: "apply option 0: cleanup entry budget must be positive"},
		{name: "segments exceed max entries", options: []Option{WithMaxEntries(2), WithSegmentCount(3)}, want: "invalid configuration: segment count must not exceed max entries"},
		{
			name: "ttl plus jitter overflow",
			options: []Option{
				WithTTL(maxDuration),
				WithJitter(time.Nanosecond),
			},
			want: "invalid configuration: ttl plus jitter exceeds maximum duration",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newSettings(test.options...)
			if err == nil || err.Error() != test.want {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSettingsAcceptsIndependentCleanupLimits(t *testing.T) {
	settings, err := newSettings(
		WithMaxEntries(4),
		WithSegmentCount(1),
		WithCleanupBatchSize(10_000),
		WithCleanupEntryBudget(3),
	)
	if err != nil {
		t.Fatalf("newSettings() error = %v", err)
	}

	if settings.cleanupBatchSize != 10_000 || settings.cleanupEntryBudget != 3 {
		t.Fatalf("cleanup limits = %d/%d, want 10000/3", settings.cleanupBatchSize, settings.cleanupEntryBudget)
	}
}

func TestSettingsAcceptsBoundaryValues(t *testing.T) {
	settings, err := newSettings(
		WithName(""),
		WithMaxEntries(1),
		WithTTL(NoExpiration),
		WithJitter(maxDuration),
	)
	if err != nil {
		t.Fatalf("newSettings() error = %v", err)
	}

	if settings.name != "" {
		t.Fatalf("name = %q, want empty", settings.name)
	}
	if settings.ttl != NoExpiration {
		t.Fatalf("ttl = %v, want NoExpiration", settings.ttl)
	}
}
