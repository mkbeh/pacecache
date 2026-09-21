package policy

import "github.com/mkbeh/pacecache"

type Policy struct {
	cache *pacecache.Cache[uint64, uint64]

	hits   uint64
	misses uint64
}

func New(capacity int, segments int) *Policy {
	return &Policy{
		cache: pacecache.New[uint64, uint64](
			pacecache.WithMaxEntries(capacity),
			pacecache.WithSegmentCount(segments),
		),
	}
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
