package pacecache

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	defaultMaxEntries = 10_000
	defaultTTL        = time.Minute
	maxDuration       = time.Duration(math.MaxInt64)
)

// Option configures a Cache.
type Option func(*cacheSettings) error

type cacheSettings struct {
	name string

	maxEntries   int
	segmentCount int

	ttl         time.Duration
	jitter      time.Duration
	negativeTTL time.Duration

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
		maxEntries:   defaultMaxEntries,
		segmentCount: defaultStorageSegmentCount,
		ttl:          defaultTTL,
	}
}

// WithMaxEntries configures the total cache entry budget.
//
// The budget is distributed across cache segments. Actual resident capacity
// may be slightly lower because each segment enforces its own local budget.
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
// More segments can reduce lock contention under concurrent access but may
// reduce effective capacity utilization because each segment has its own
// entry budget.
func WithSegmentCount(count int) Option {
	return func(settings *cacheSettings) error {
		if count <= 0 {
			return errors.New("segment count must be positive")
		}

		settings.segmentCount = count

		return nil
	}
}

// WithTTL configures the lifetime of positive cache entries.
//
// Cache entries use absolute expiration. Cache hits do not extend TTL.
func WithTTL(ttl time.Duration) Option {
	return func(settings *cacheSettings) error {
		if ttl <= 0 {
			return errors.New("ttl must be positive")
		}

		settings.ttl = ttl

		return nil
	}
}

// WithJitter configures random positive TTL spread.
//
// Jitter is added to positive entry TTL to reduce synchronized expiration.
// Zero disables jitter.
func WithJitter(jitter time.Duration) Option {
	return func(settings *cacheSettings) error {
		if jitter < 0 {
			return errors.New("jitter must not be negative")
		}

		settings.jitter = jitter

		return nil
	}
}

// WithNegativeTTL configures the lifetime of cached not-found results.
//
// Zero disables negative caching. Negative TTL is not jittered.
func WithNegativeTTL(ttl time.Duration) Option {
	return func(settings *cacheSettings) error {
		if ttl < 0 {
			return errors.New("negative ttl must not be negative")
		}

		settings.negativeTTL = ttl

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
	if strings.TrimSpace(settings.name) == "" {
		return errors.New("cache name must not be blank")
	}

	if settings.ttl > maxDuration-settings.jitter {
		return errors.New("ttl plus jitter exceeds maximum duration")
	}

	if settings.segmentCount > settings.maxEntries {
		return errors.New("segment count must not exceed max entries")
	}

	return nil
}
