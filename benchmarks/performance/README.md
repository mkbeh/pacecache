# Performance Benchmarks

This directory contains the benchmarking suite and simulation tools used to evaluate the performance characteristics,
cache efficiency, and memory consumption of `pacecache`.

The benchmarks focus on three core metrics:

* **Throughput:** Concurrent read/write performance under different workload mixes.
* **Hit Ratio:** Cache efficiency across different capacities under a Zipfian access pattern.
* **Memory Consumption:** Live heap usage across different cache capacities.

---

## Throughput Benchmarks

This benchmark evaluates `pacecache` under concurrent read/write workloads. It uses a pre-generated Scrambled Zipfian
access pattern to create skewed key access and contention similar to hot-key workloads.

### Configuration

* Workers: 8 parallel goroutines
* Segments: 512
* Maximum entries: 10K, 100K, and 1M
* Write ratios: 0%, 25%, 50%, 75%, and 100%
* Expiration: Disabled to isolate cache operation throughput

### Execution

```bash
go test ./benchmarks/performance/throughput \
  -run '^$' \
  -bench '^BenchmarkThroughput$' \
  -benchmem \
  -benchtime=3s \
  -count=5 \
  -cpu=8 \
  | tee ./benchmarks/performance/throughput/results/throughput_cpu8_segments512.txt
```

---

## Hit Ratio Simulation

The simulator measures how cache capacity affects the hit ratio under a Zipfian access pattern.

### Configuration

* Accesses: 1,000,000
* Segments: 32
* Capacities: 500, 1K, 2K, 5K, 10K, 20K, 40K, and 80K entries
* Expiration: Disabled to isolate capacity and eviction behavior

### Execution

```bash
go run ./benchmarks/performance/hitratio/cmd \
  -config ./benchmarks/performance/hitratio/configs/zipf.toml \
  | tee ./benchmarks/performance/hitratio/results/hit_ratio_zipf.txt
```

---

## Memory Consumption

This benchmark measures live heap consumption after populating the cache with fixed-size keys and values.

### Configuration

* Segments: 32
* Data: Fixed 32-byte keys and 32-byte values
* Capacities: 1K, 10K, 25K, 100K, and 1M entries
* Workload: 10 reads and a 5µs settle delay after each insertion
* Expiration: 1-hour TTL

### Execution

```bash
./benchmarks/performance/memory/bench.sh \
  | tee ./benchmarks/performance/memory/results/memory.txt
```

---

## Chart Generation

Each benchmark persists its raw output within its local `results` directory. Once the benchmark runs are complete, you
can generate the corresponding charts independently:

```bash
go run ./benchmarks/performance/throughput/charts
go run ./benchmarks/performance/hitratio/charts
go run ./benchmarks/performance/memory/charts
````

These commands generate visual artifacts at the following locations:

```text
benchmarks/performance/
├── throughput/assets/throughput.png
├── hitratio/assets/hit-ratio.png
└── memory/assets/memory.png
```