package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"

	perfchart "github.com/mkbeh/pacecache/benchmarks/performance/internal/chart"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
)

var benchmarkLine = regexp.MustCompile(
	`^BenchmarkThroughput/max_entries_(\d+)/writes_(\d+)pct-\d+\s+\d+\s+[0-9.]+\s+ns/op\s+([0-9.]+)\s+ops/s`,
)

type benchmarkKey struct {
	capacity int
	writes   int
}

func main() {
	input := flag.String(
		"input",
		"./benchmarks/performance/throughput/results/throughput_cpu8_segments512.txt",
		"path to throughput benchmark result",
	)
	output := flag.String(
		"output",
		"./benchmarks/performance/throughput/assets/throughput.png",
		"output plot path",
	)
	flag.Parse()

	if err := run(*input, *output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(input, output string) error {
	results, err := readResults(input)
	if err != nil {
		return err
	}

	capacities := uniqueCapacities(results)
	writeRatios := uniqueWriteRatios(results)

	p := perfchart.New(
		"Throughput",
		"Write ratio (%)",
		"Throughput (M ops/s)",
	)
	p.X.Min = 0
	p.X.Max = 100
	p.X.Tick.Marker = percentTicks(writeRatios)

	minThroughput := math.Inf(1)
	maxThroughput := math.Inf(-1)

	for index, capacity := range capacities {
		points := make(plotter.XYs, 0, len(writeRatios))
		for _, writes := range writeRatios {
			values := results[benchmarkKey{capacity: capacity, writes: writes}]
			if len(values) == 0 {
				return fmt.Errorf("missing throughput results for capacity=%d writes=%d", capacity, writes)
			}

			throughput := median(values) / 1_000_000
			minThroughput = math.Min(minThroughput, throughput)
			maxThroughput = math.Max(maxThroughput, throughput)

			points = append(points, plotter.XY{
				X: float64(writes),
				Y: throughput,
			})
		}

		line, scatter, err := plotter.NewLinePoints(points)
		if err != nil {
			return fmt.Errorf("create throughput series: %w", err)
		}

		perfchart.StyleLinePoints(line, scatter, index)

		p.Add(line, scatter)
		p.Legend.Add(perfchart.CapacityLabel(capacity), line, scatter)
	}

	setThroughputYRange(p, minThroughput, maxThroughput)

	return perfchart.Save(p, output)
}

func readResults(path string) (map[benchmarkKey][]float64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open throughput result: %w", err)
	}
	defer file.Close()

	results := make(map[benchmarkKey][]float64)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		match := benchmarkLine.FindStringSubmatch(scanner.Text())
		if match == nil {
			continue
		}

		capacity, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("parse capacity %q: %w", match[1], err)
		}
		writes, err := strconv.Atoi(match[2])
		if err != nil {
			return nil, fmt.Errorf("parse write ratio %q: %w", match[2], err)
		}
		ops, err := strconv.ParseFloat(match[3], 64)
		if err != nil {
			return nil, fmt.Errorf("parse ops/s %q: %w", match[3], err)
		}

		key := benchmarkKey{capacity: capacity, writes: writes}
		results[key] = append(results[key], ops)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read throughput result: %w", err)
	}
	if len(results) == 0 {
		return nil, errors.New("no throughput benchmark rows found")
	}

	return results, nil
}

func uniqueCapacities(results map[benchmarkKey][]float64) []int {
	set := make(map[int]struct{})
	for key := range results {
		set[key.capacity] = struct{}{}
	}

	values := make([]int, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Ints(values)
	return values
}

func uniqueWriteRatios(results map[benchmarkKey][]float64) []int {
	set := make(map[int]struct{})
	for key := range results {
		set[key.writes] = struct{}{}
	}

	values := make([]int, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Ints(values)
	return values
}

func percentTicks(values []int) plot.ConstantTicks {
	ticks := make(plot.ConstantTicks, 0, len(values))
	for _, value := range values {
		ticks = append(ticks, plot.Tick{
			Value: float64(value),
			Label: strconv.Itoa(value),
		})
	}
	return ticks
}

func median(values []float64) float64 {
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)

	middle := len(copyValues) / 2
	if len(copyValues)%2 == 1 {
		return copyValues[middle]
	}
	return (copyValues[middle-1] + copyValues[middle]) / 2
}

func setThroughputYRange(p *plot.Plot, minValue, maxValue float64) {
	if math.IsInf(minValue, 0) || math.IsInf(maxValue, 0) {
		return
	}

	span := maxValue - minValue
	if span <= 0 {
		span = math.Max(1, maxValue*0.1)
	}

	padding := span * 0.12
	const tickStep = 5.0

	p.Y.Min = math.Max(0, math.Floor((minValue-padding)/tickStep)*tickStep)
	p.Y.Max = math.Ceil((maxValue+padding)/tickStep) * tickStep
}
