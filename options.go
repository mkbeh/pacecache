package pacecache

import (
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	defaultMaxEntries = 10_000
	defaultTTL        = NoExpiration
	maxDuration       = time.Duration(math.MaxInt64)
)

// Option configures a Cache created by New or NewWithDefaultLoader.
type Option func(*cacheSettings) error

type cacheSettings struct {
	name string

	maxEntries   int
	segmentCount int

	ttl               time.Duration
	jitter            time.Duration
	slidingExpiration bool

	cleanupInterval    time.Duration
	cleanupBatchSize   int
	cleanupEntryBudget int

	metrics Metrics
}

func newCacheSettings(name string, options ...Option) (*cacheSettings, error) {
	settings := defaultCacheSettings()
	settings.name = name

	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf("option %d is nil", index)
		}

		if err := option(settings); err != nil {
			return nil, fmt.Errorf("apply option %d: %w", index, err)
		}
	}

	if err := settings.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return settings, nil
}

func defaultCacheSettings() *cacheSettings {
	return &cacheSettings{
		maxEntries:         defaultMaxEntries,
		segmentCount:       defaultStorageSegmentCount,
		ttl:                defaultTTL,
		cleanupBatchSize:   defaultCleanupBatchSize,
		cleanupEntryBudget: defaultCleanupEntryBudget,
	}
}

// WithMaxEntries configures the total cache entry budget.
//
// With one segment, the full budget is shared by the cache. When multiple
// segments are configured, the budget is distributed across them and effective
// capacity utilization may be slightly lower because each segment enforces its
// own local budget.
func WithMaxEntries(maxEntries int) Option {
	return func(settings *cacheSettings) error {
		if maxEntries <= 0 {
			return errors.New("max entries must be positive")
		}

		settings.maxEntries = maxEntries

		return nil
	}
}

// WithSegmentCount configures the number of independent cache segments.
//
// The default is one segment. More segments can reduce lock contention under
// concurrent access but may reduce effective capacity utilization because each
// segment has its own entry budget. Benchmark segment counts against the
// application's actual workload.
func WithSegmentCount(count int) Option {
	return func(settings *cacheSettings) error {
		if count <= 0 {
			return errors.New("segment count must be positive")
		}

		settings.segmentCount = count

		return nil
	}
}

// WithTTL configures the default lifetime of cache entries.
//
// A positive TTL enables time-based expiration. NoExpiration disables
// time-based expiration for entries using the default expiration.
func WithTTL(ttl time.Duration) Option {
	return func(settings *cacheSettings) error {
		if ttl <= 0 && ttl != NoExpiration {
			return errors.New("ttl must be positive or NoExpiration")
		}

		settings.ttl = ttl

		return nil
	}
}

// WithJitter configures random TTL spread.
//
// Jitter adds a random duration smaller than the configured value when an
// expiring entry is stored, reducing synchronized expiration. With sliding
// expiration, the resulting effective TTL is reused on every refresh instead of
// selecting another jitter value. Zero disables jitter.
func WithJitter(jitter time.Duration) Option {
	return func(settings *cacheSettings) error {
		if jitter < 0 {
			return errors.New("jitter must not be negative")
		}

		settings.jitter = jitter

		return nil
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
	return func(settings *cacheSettings) error {
		settings.slidingExpiration = true

		return nil
	}
}

// WithCleanupInterval enables periodic background physical removal of expired
// entries.
//
// The interval controls regular background cleanup wakeups. While expired
// backlog remains, the cleaner may schedule bounded continuation work sooner.
// The interval does not affect logical TTL precision or the internal
// expiration bucket resolution.
//
// Background cleanup is disabled by default. This is the only cleanup option
// that starts a background worker; cleanup batch size and entry budget only
// configure cleanup behavior. Manual cleanup through Cache.DeleteExpired is
// always available without this option. When background cleanup is enabled,
// Close must be called to stop the cleaner goroutine.
func WithCleanupInterval(interval time.Duration) Option {
	return func(settings *cacheSettings) error {
		if interval <= 0 {
			return errors.New("cleanup interval must be positive")
		}

		settings.cleanupInterval = interval

		return nil
	}
}

// WithCleanupBatchSize configures the maximum number of expired entries
// removed from one storage segment in a single cleanup batch.
//
// The setting applies to both manual and background cleanup. Larger batches
// can increase cleanup throughput but may hold a segment lock for longer.
// Values larger than a segment or the remaining cleanup budget are safe and
// are naturally limited by the available work. The default is 256.
func WithCleanupBatchSize(size int) Option {
	return func(settings *cacheSettings) error {
		if size <= 0 {
			return errors.New("cleanup batch size must be positive")
		}

		settings.cleanupBatchSize = size

		return nil
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
// cache size are safe. The default is 16384.
func WithCleanupEntryBudget(entries int) Option {
	return func(settings *cacheSettings) error {
		if entries <= 0 {
			return errors.New("cleanup entry budget must be positive")
		}

		settings.cleanupEntryBudget = entries

		return nil
	}
}

// WithMetrics configures optional cache metrics.
//
// The Metrics implementation may be reused by multiple caches. Its
// registration is released when Cache.Close is called.
func WithMetrics(metrics Metrics) Option {
	return func(settings *cacheSettings) error {
		settings.metrics = metrics

		return nil
	}
}

func (settings *cacheSettings) validate() error {
	if settings.name == "" {
		return errors.New("cache name must not be empty")
	}

	if settings.ttl > 0 && settings.ttl > maxDuration-settings.jitter {
		return errors.New("ttl plus jitter exceeds maximum duration")
	}

	if settings.segmentCount > settings.maxEntries {
		return errors.New("segment count must not exceed max entries")
	}

	return nil
}
