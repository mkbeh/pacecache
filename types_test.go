package pacecache

import (
	"testing"
	"time"
)

func TestEntryAccessors(t *testing.T) {
	expiresAt := time.Now().Add(time.Minute)
	entry := Entry[int]{value: 42, expiresAt: expiresAt}

	if got := entry.Value(); got != 42 {
		t.Fatalf("Value() = %d, want 42", got)
	}
	if got := entry.ExpiresAt(); !got.Equal(expiresAt) {
		t.Fatalf("ExpiresAt() = %v, want %v", got, expiresAt)
	}

	var zero Entry[int]
	if zero.Value() != 0 || !zero.ExpiresAt().IsZero() {
		t.Fatalf("zero Entry = value:%d expiresAt:%v", zero.Value(), zero.ExpiresAt())
	}
}
