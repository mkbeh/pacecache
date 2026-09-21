package pacecache

import (
	"time"
)

const (
	defaultMaxEntries = 10_000
	defaultTTL        = NoExpiration
)

// Option configures a Cache created by New or NewWithLoader.
type Option func(*settings)

type settings struct {
	name string

	maxEntries   int
	segmentCount int

	ttl               time.Duration
	jitter            time.Duration
	slidingExpiration bool

	cleanupInterval    time.Duration
	cleanupBatchSize   int
	cleanupEntryBudget int
}

func newSettings(options ...Option) *settings {
	settings := defaultSettings()

	for _, option := range options {
		if option != nil {
			option(settings)
		}
	}

	if settings.segmentCount > settings.maxEntries {
		settings.segmentCount = settings.maxEntries
	}

	return settings
}

func defaultSettings() *settings {
	return &settings{
		maxEntries:         defaultMaxEntries,
		segmentCount:       defaultStorageSegmentCount,
		ttl:                defaultTTL,
		cleanupInterval:    defaultCleanupInterval,
		cleanupBatchSize:   defaultCleanupBatchSize,
		cleanupEntryBudget: defaultCleanupEntryBudget,
	}
}

// WithName configures an optional logical cache name.
//
// The name can be used to identify the cache in logs, diagnostics, or
// observability integrations. An empty name leaves the cache unnamed.
func WithName(name string) Option {
	return func(settings *settings) {
		settings.name = name
	}
}

// WithMaxEntries configures the total cache entry budget.
//
// With one segment, the full budget is shared by the cache. When multiple
// segments are configured, the budget is distributed across them and effective
// capacity utilization may be slightly lower because each segment enforces its
// own local budget. Non-positive values are ignored.
func WithMaxEntries(maxEntries int) Option {
	return func(settings *settings) {
		if maxEntries > 0 {
			settings.maxEntries = maxEntries
		}
	}
}

// WithSegmentCount configures the number of independent cache segments.
//
// The default is one segment. More segments can reduce lock contention under
// concurrent access but may reduce effective capacity utilization because each
// segment has its own entry budget. Benchmark segment counts against the
// application's actual workload. Non-positive values are ignored. Values above
// the configured entry budget are clamped to that budget.
func WithSegmentCount(count int) Option {
	return func(settings *settings) {
		if count > 0 {
			settings.segmentCount = count
		}
	}
}

// WithTTL configures the default lifetime of cache entries.
//
// A positive TTL enables time-based expiration. NoExpiration disables
// time-based expiration for entries using the default expiration. Other
// non-positive values are ignored.
func WithTTL(ttl time.Duration) Option {
	return func(settings *settings) {
		if ttl == NoExpiration || ttl > 0 {
			settings.ttl = ttl
		}
	}
}

// WithJitter configures random TTL spread.
//
// Jitter adds a random duration smaller than the configured value when an
// expiring entry is stored, reducing synchronized expiration. With sliding
// expiration, the resulting effective TTL is reused on every refresh instead of
// selecting another jitter value. Zero disables jitter. Negative values are
// ignored.
func WithJitter(jitter time.Duration) Option {
	return func(settings *settings) {
		if jitter >= 0 {
			settings.jitter = jitter
		}
	}
}

// WithSlidingExpiration refreshes the expiration deadline of live expiring
// entries whenever they are successfully read.
//
// Each entry is refreshed using the effective TTL selected when it was stored.
// Entries using DefaultExpiration derive that TTL from the cache configuration,
// while entries with an explicit positive TTL retain their own TTL. Configured
// jitter is selected once when the entry is stored and reused by subsequent
// refreshes. Entries using NoExpiration are not refreshed.
func WithSlidingExpiration() Option {
	return func(settings *settings) {
		settings.slidingExpiration = true
	}
}

// WithCleanupInterval configures the interval between regular cleanup wakeups.
//
// The default is one minute. While expired backlog remains, the cleaner may
// schedule bounded continuation work sooner. The interval does not affect
// logical TTL precision or the internal expiration bucket resolution.
//
// Background cleanup must be started explicitly with StartCleanup. Manual
// cleanup through Cache.DeleteExpired is always available. Non-positive values
// are ignored.
func WithCleanupInterval(interval time.Duration) Option {
	return func(settings *settings) {
		if interval > 0 {
			settings.cleanupInterval = interval
		}
	}
}

// WithCleanupBatchSize configures the maximum number of expired entries
// removed from one storage segment in a single cleanup batch.
//
// The setting applies to both manual and background cleanup. Larger batches
// can increase cleanup throughput but may hold a segment lock for longer.
// Values larger than a segment or the remaining cleanup budget are safe and
// are naturally limited by the available work. The default is 256.
// Non-positive values are ignored.
func WithCleanupBatchSize(size int) Option {
	return func(settings *settings) {
		if size > 0 {
			settings.cleanupBatchSize = size
		}
	}
}

// WithCleanupEntryBudget configures the maximum number of expired entries
// removed during one cooperative cleanup quantum.
//
// The setting applies to both manual and background cleanup. Larger budgets
// allow large expiration backlogs to be drained more aggressively. Background
// cleanup is additionally bounded by an internal time budget. Manual cleanup
// yields cooperatively after exhausting the entry budget and continues until
// all entries due at the start of the call are drained. Values larger than the
// cache size are safe. The default is 16384. Non-positive values are ignored.
func WithCleanupEntryBudget(entries int) Option {
	return func(settings *settings) {
		if entries > 0 {
			settings.cleanupEntryBudget = entries
		}
	}
}
