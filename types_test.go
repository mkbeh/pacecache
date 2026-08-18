package pacecache

import (
	"testing"
	"time"
)

func TestEntry(t *testing.T) {
	expiresAt := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	entry := Entry[string]{
		value:     "value",
		expiresAt: expiresAt,
	}

	if got := entry.Value(); got != "value" {
		t.Fatalf("Value() = %q, want %q", got, "value")
	}
	if got := entry.ExpiresAt(); !got.Equal(expiresAt) {
		t.Fatalf("ExpiresAt() = %v, want %v", got, expiresAt)
	}

	var zero Entry[string]
	if got := zero.Value(); got != "" {
		t.Fatalf("zero Value() = %q, want empty string", got)
	}
	if got := zero.ExpiresAt(); !got.IsZero() {
		t.Fatalf("zero ExpiresAt() = %v, want zero time", got)
	}
}

func TestNewLoadResult(t *testing.T) {
	tests := []struct {
		name         string
		value        int
		found        bool
		deadline     int64
		wantValue    int
		wantDeadline int64
		wantStatus   LookupStatus
	}{
		{
			name:         "positive expiring",
			value:        42,
			found:        true,
			deadline:     100,
			wantValue:    42,
			wantDeadline: 100,
			wantStatus:   LookupHit,
		},
		{
			name:       "positive without expiration",
			value:      42,
			found:      true,
			wantValue:  42,
			wantStatus: LookupHit,
		},
		{
			name:         "cached negative",
			value:        42,
			deadline:     100,
			wantDeadline: 100,
			wantStatus:   LookupNegativeHit,
		},
		{
			name:       "uncached negative",
			value:      42,
			wantStatus: LookupMiss,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := newLoadResult(test.value, test.found, test.deadline)

			if result.value != test.wantValue {
				t.Fatalf("value = %d, want %d", result.value, test.wantValue)
			}
			if result.deadline != test.wantDeadline {
				t.Fatalf("deadline = %d, want %d", result.deadline, test.wantDeadline)
			}
			if result.status != test.wantStatus {
				t.Fatalf("status = %v, want %v", result.status, test.wantStatus)
			}
		})
	}
}

func TestLoadResultFromCached(t *testing.T) {
	tests := []struct {
		name       string
		cached     cachedEntry[int]
		wantStatus LookupStatus
	}{
		{
			name: "positive",
			cached: cachedEntry[int]{
				value:    42,
				found:    true,
				deadline: 100,
			},
			wantStatus: LookupHit,
		},
		{
			name: "negative",
			cached: cachedEntry[int]{
				deadline: 100,
			},
			wantStatus: LookupNegativeHit,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := loadResultFromCached(test.cached)

			if result.value != test.cached.value {
				t.Fatalf("value = %d, want %d", result.value, test.cached.value)
			}
			if result.deadline != test.cached.deadline {
				t.Fatalf("deadline = %d, want %d", result.deadline, test.cached.deadline)
			}
			if result.status != test.wantStatus {
				t.Fatalf("status = %v, want %v", result.status, test.wantStatus)
			}
		})
	}
}
