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

func TestLoadResultFromCached(t *testing.T) {
	cached := cachedEntry[int]{
		value:    42,
		deadline: 100,
	}

	result := loadResultFromCached(cached)

	if result.value != cached.value {
		t.Fatalf("value = %d, want %d", result.value, cached.value)
	}
	if result.deadline != cached.deadline {
		t.Fatalf("deadline = %d, want %d", result.deadline, cached.deadline)
	}
	if !result.found {
		t.Fatal("found = false, want true")
	}
}
