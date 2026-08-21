package throughput

import (
	"math/rand"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mkbeh/pacecache"
	"github.com/pingcap/go-ycsb/pkg/generator"
)

const (
	throughputWorkers  = 8
	throughputSegments = 512
)

var throughputMaxEntries = [...]int{
	10_000,
	100_000,
	1_000_000,
}

type throughputCase struct {
	name            string
	writePercentage uint64
}

var throughputCases = [...]throughputCase{
	{name: "writes_0pct", writePercentage: 0},
	{name: "writes_25pct", writePercentage: 25},
	{name: "writes_50pct", writePercentage: 50},
	{name: "writes_75pct", writePercentage: 75},
	{name: "writes_100pct", writePercentage: 100},
}

type throughputData struct {
	keys   []string
	values []string
}

func BenchmarkThroughput(b *testing.B) {
	if got := runtime.GOMAXPROCS(0); got != throughputWorkers {
		b.Skipf(
			"throughput benchmark requires GOMAXPROCS=%d, got %d",
			throughputWorkers,
			got,
		)
	}

	for _, maxEntries := range throughputMaxEntries {
		data := newThroughputData(maxEntries)

		b.Run("max_entries_"+strconv.Itoa(maxEntries), func(b *testing.B) {
			for _, benchmark := range throughputCases {
				b.Run(benchmark.name, func(b *testing.B) {
					runThroughputBenchmark(
						b,
						maxEntries,
						benchmark.writePercentage,
						data,
					)
				})
			}
		})
	}
}

func runThroughputBenchmark(
	b *testing.B,
	maxEntries int,
	writePercentage uint64,
	data throughputData,
) {
	b.Helper()

	cache, err := pacecache.New[string, string](
		"throughput",
		pacecache.WithMaxEntries(maxEntries),
		pacecache.WithSegmentCount(throughputSegments),
	)
	if err != nil {
		b.Fatalf("create cache: %v", err)
	}
	b.Cleanup(cache.Close)

	for index, key := range data.keys {
		cache.Set(key, data.values[index], pacecache.NoExpiration)
	}

	if evictions := cache.Stats().EvictionCount; evictions != 0 {
		b.Fatalf("population caused %d evictions", evictions)
	}

	var workers atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()

	startedAt := time.Now()

	b.RunParallel(func(pb *testing.PB) {
		worker := workers.Add(1)
		index := workerStartIndex(worker, len(data.keys))

		if isWriter(writePercentage, worker) {
			for pb.Next() {
				cache.Set(
					data.keys[index],
					data.values[index],
					pacecache.NoExpiration,
				)
				index = nextIndex(index, len(data.keys))
			}
			return
		}

		for pb.Next() {
			cache.Get(data.keys[index])
			index = nextIndex(index, len(data.keys))
		}
	})

	elapsed := time.Since(startedAt)

	b.StopTimer()

	if got := workers.Load(); got != throughputWorkers {
		b.Fatalf("workers = %d, want %d", got, throughputWorkers)
	}

	if evictions := cache.Stats().EvictionCount; evictions != 0 {
		b.Fatalf("benchmark caused %d evictions", evictions)
	}

	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
}

func newThroughputData(length int) throughputData {
	random := rand.New(rand.NewSource(1))
	zipf := generator.NewScrambledZipfian(
		0,
		int64(length/3),
		generator.ZipfianConstant,
	)

	keys := make([]string, length)
	values := make([]string, length)

	for index := range length {
		key := int(zipf.Next(random))

		keys[index] = "pacecache-throughput-key-" + strconv.Itoa(key)
		values[index] = "pacecache-throughput-value-" + strconv.Itoa(index)
	}

	return throughputData{
		keys:   keys,
		values: values,
	}
}

func isWriter(writePercentage, worker uint64) bool {
	return writePercentage*worker/100 !=
		writePercentage*(worker-1)/100
}

func workerStartIndex(worker uint64, length int) int {
	return int((worker - 1) * uint64(length) / throughputWorkers)
}

func nextIndex(index, length int) int {
	index++
	if index == length {
		return 0
	}

	return index
}
