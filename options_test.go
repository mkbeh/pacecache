package pacecache

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestNewCacheSettingsDefaults(t *testing.T) {
	settings, err := newCacheSettings("users")
	if err != nil {
		t.Fatalf("newCacheSettings() error = %v", err)
	}

	if settings.name != "users" {
		t.Fatalf("name = %q, want %q", settings.name, "users")
	}
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
	if settings.negativeTTL != 0 {
		t.Fatalf("negativeTTL = %v, want 0", settings.negativeTTL)
	}
	if settings.metrics != nil {
		t.Fatal("metrics is not nil")
	}
}

func TestNewCacheSettingsOptions(t *testing.T) {
	metrics := &testMetrics{}

	settings, err := newCacheSettings(
		"users",
		WithMaxEntries(128),
		WithSegmentCount(8),
		WithTTL(2*time.Minute),
		WithJitter(10*time.Second),
		WithNegativeTTL(30*time.Second),
		WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("newCacheSettings() error = %v", err)
	}

	if settings.maxEntries != 128 {
		t.Fatalf("maxEntries = %d, want 128", settings.maxEntries)
	}
	if settings.segmentCount != 8 {
		t.Fatalf("segmentCount = %d, want 8", settings.segmentCount)
	}
	if settings.ttl != 2*time.Minute {
		t.Fatalf("ttl = %v, want %v", settings.ttl, 2*time.Minute)
	}
	if settings.jitter != 10*time.Second {
		t.Fatalf("jitter = %v, want %v", settings.jitter, 10*time.Second)
	}
	if settings.negativeTTL != 30*time.Second {
		t.Fatalf("negativeTTL = %v, want %v", settings.negativeTTL, 30*time.Second)
	}
	if settings.metrics != metrics {
		t.Fatal("metrics option was not applied")
	}
}

func TestNewCacheSettingsRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		cache   string
		options []Option
		want    string
	}{
		{
			name:  "blank name",
			cache: " \t ",
			want:  "cache name must not be blank",
		},
		{
			name:    "nil option",
			cache:   "users",
			options: []Option{nil},
			want:    "option 0 is nil",
		},
		{
			name:    "max entries",
			cache:   "users",
			options: []Option{WithMaxEntries(0)},
			want:    "max entries must be positive",
		},
		{
			name:    "segment count",
			cache:   "users",
			options: []Option{WithSegmentCount(0)},
			want:    "segment count must be positive",
		},
		{
			name:    "ttl",
			cache:   "users",
			options: []Option{WithTTL(0)},
			want:    "ttl must be positive",
		},
		{
			name:    "negative jitter",
			cache:   "users",
			options: []Option{WithJitter(-time.Nanosecond)},
			want:    "jitter must not be negative",
		},
		{
			name:    "negative ttl",
			cache:   "users",
			options: []Option{WithNegativeTTL(-time.Nanosecond)},
			want:    "negative ttl must not be negative",
		},
		{
			name:  "ttl plus jitter overflow",
			cache: "users",
			options: []Option{
				WithTTL(time.Duration(math.MaxInt64)),
				WithJitter(time.Nanosecond),
			},
			want: "ttl plus jitter exceeds maximum duration",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newCacheSettings(test.cache, test.options...)
			if err == nil {
				t.Fatal("newCacheSettings() error = nil")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %q, want substring %q", err, test.want)
			}
		})
	}
}

func TestNewCacheSettingsWrapsOptionError(t *testing.T) {
	sentinel := errors.New("sentinel")

	_, err := newCacheSettings(
		"users",
		func(*cacheSettings) error { return sentinel },
	)
	if err == nil {
		t.Fatal("newCacheSettings() error = nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want errors.Is(..., sentinel)", err)
	}
}
