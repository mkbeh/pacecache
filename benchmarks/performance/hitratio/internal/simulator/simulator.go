package simulator

import (
	"fmt"
	"io"

	"github.com/mkbeh/pacecache/benchmarks/performance/hitratio/internal/config"
	"github.com/mkbeh/pacecache/benchmarks/performance/hitratio/internal/policy"
	"github.com/mkbeh/pacecache/benchmarks/performance/hitratio/internal/trace"
)

type Simulator struct {
	cfg config.Config
}

func New(cfg config.Config) Simulator {
	return Simulator{
		cfg: cfg,
	}
}

func (s Simulator) Simulate(output io.Writer) error {
	fmt.Fprintln(
		output,
		"trace,capacity,segments,requests,hits,misses,hit_ratio",
	)

	for _, unsignedCapacity := range s.cfg.Capacities {
		capacity := int(unsignedCapacity)

		result, err := s.simulateCapacity(capacity)
		if err != nil {
			return err
		}

		fmt.Fprintf(
			output,
			"%s,%d,%d,%d,%d,%d,%.4f\n",
			s.cfg.Name,
			capacity,
			s.cfg.Segments,
			result.Requests,
			result.Hits,
			result.Misses,
			result.Ratio,
		)
	}

	return nil
}

type result struct {
	Requests uint64
	Hits     uint64
	Misses   uint64
	Ratio    float64
}

func (s Simulator) simulateCapacity(capacity int) (result, error) {
	p, err := policy.New(capacity, s.cfg.Segments)
	if err != nil {
		return result{}, fmt.Errorf("create policy for capacity %d: %w", capacity, err)
	}
	defer p.Close()

	generator := trace.NewZipf(
		s.cfg.Zipf.S,
		s.cfg.Zipf.V,
		s.cfg.Zipf.IMAX,
		*s.cfg.Limit,
	)

	for {
		key, ok := generator.Next()
		if !ok {
			break
		}

		p.Record(key)
	}

	hits := p.Hits()
	misses := p.Misses()

	return result{
		Requests: hits + misses,
		Hits:     hits,
		Misses:   misses,
		Ratio:    p.Ratio(),
	}, nil
}
