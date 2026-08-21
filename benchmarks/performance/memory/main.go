package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/mkbeh/pacecache"
)

const (
	segmentCount  = 1
	expiration    = time.Hour
	readsPerEntry = 10
	settleDelay   = 5 * time.Microsecond
)

func main() {
	capacity := flag.Int(
		"capacity",
		0,
		"maximum number of cache entries",
	)
	flag.Parse()

	if *capacity <= 0 {
		log.Fatal("capacity must be greater than zero")
	}

	keys, values := makeDataset(*capacity)

	// Exclude the pre-generated benchmark dataset from cache memory.
	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	cache, err := pacecache.New[string, string](
		"memory",
		pacecache.WithMaxEntries(*capacity),
		pacecache.WithSegmentCount(segmentCount),
		pacecache.WithTTL(expiration),
	)
	if err != nil {
		log.Fatalf("create cache: %v", err)
	}
	defer cache.Close()

	for index := range *capacity {
		key := keys[index]

		cache.Set(
			key,
			values[index],
			pacecache.DefaultExpiration,
		)

		for range readsPerEntry {
			if _, found := cache.Get(key); !found {
				log.Fatalf("key %q not found after Set", key)
			}
		}

		time.Sleep(settleDelay)
	}

	// Measure live cache memory after transient allocations are reclaimed.
	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	stats := cache.Stats()

	// Keep the benchmark dataset alive across both memory snapshots.
	runtime.KeepAlive(keys)
	runtime.KeepAlive(values)
	runtime.KeepAlive(cache)

	if after.Alloc < before.Alloc {
		log.Fatalf(
			"heap allocation decreased: before=%d after=%d",
			before.Alloc,
			after.Alloc,
		)
	}

	allocBytes := after.Alloc - before.Alloc
	totalAllocBytes := after.TotalAlloc - before.TotalAlloc

	var bytesPerEntry float64
	if stats.EntryCount != 0 {
		bytesPerEntry =
			float64(allocBytes) / float64(stats.EntryCount)
	}

	fmt.Printf(
		"%d,%d,%d,%d,%d,%.2f\n",
		*capacity,
		stats.EntryCount,
		segmentCount,
		allocBytes,
		totalAllocBytes,
		bytesPerEntry,
	)
}

func makeDataset(capacity int) ([]string, []string) {
	keys := make([]string, capacity)
	values := make([]string, capacity)

	for index := range capacity {
		// Exactly 32 bytes for all benchmark capacities.
		keys[index] = fmt.Sprintf("key-%028d", index)
		values[index] = fmt.Sprintf("value-%026d", index)
	}

	return keys, values
}
