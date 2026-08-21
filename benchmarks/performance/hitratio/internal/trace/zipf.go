package trace

import (
	"math/rand"
	"time"
)

type Zipf struct {
	source    *rand.Zipf
	remaining uint
}

func NewZipf(s float64, v float64, imax uint64, limit uint) *Zipf {
	random := rand.New(
		rand.NewSource(time.Now().UnixNano()),
	)

	return &Zipf{
		source:    rand.NewZipf(random, s, v, imax),
		remaining: limit,
	}
}

func (z *Zipf) Next() (uint64, bool) {
	if z.remaining == 0 {
		return 0, false
	}

	z.remaining--

	return z.source.Uint64(), true
}
