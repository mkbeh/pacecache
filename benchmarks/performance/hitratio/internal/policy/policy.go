package policy

import (
	"fmt"

	"github.com/mkbeh/pacecache"
)

type Policy struct {
	cache *pacecache.Cache[uint64, uint64]

	hits   uint64
	misses uint64
}

func New(capacity int, segments int) (*Policy, error) {
	cache, err := pacecache.New[uint64, uint64](
		"hit-ratio",
		pacecache.WithMaxEntries(capacity),
		pacecache.WithSegmentCount(segments),
	)
	if err != nil {
		return nil, fmt.Errorf("create cache: %w", err)
	}

	return &Policy{
		cache: cache,
	}, nil
}

func (p *Policy) Record(key uint64) {
	value, found := p.cache.Get(key)
	if found {
		if value != key {
			panic("cache returned invalid value")
		}

		p.hits++

		return
	}

	p.cache.Set(key, key, pacecache.NoExpiration)
	p.misses++
}

func (p *Policy) Hits() uint64 {
	return p.hits
}

func (p *Policy) Misses() uint64 {
	return p.misses
}

func (p *Policy) Ratio() float64 {
	total := p.hits + p.misses
	if total == 0 {
		return 0
	}

	return 100 * float64(p.hits) / float64(total)
}

func (p *Policy) Close() {
	p.cache.Close()
}
