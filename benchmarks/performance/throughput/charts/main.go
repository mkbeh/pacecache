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
	"gonum.org/v1/plot/vg"
)

var benchmarkLine = regexp.MustCompile(
	`^BenchmarkThroughput/max_entries_(\d+)/writes_(\d+)pct-\d+\s+\d+\s+[0-9.]+\s+ns/op\s+([0-9.]+)\s+ops/s`,
)

type benchmarkKey struct {
	maxEntries int
	writeRatio int
}

func main() {
	input := flag.String(
		"input",
		"./benchmarks/performance/throughput/results/throughput.txt",
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

	maxEntriesValues := uniqueMaxEntries(results)
	writeRatios := uniqueWriteRatios(results)

	p := perfchart.New(
		"Throughput by Write Ratio",
		"Write ratio (%)",
		"Throughput (M ops/s)",
	)

	barWidth := vg.Points(32)
	barGap := vg.Points(6)

	var maxThroughput float64
	seriesStride := barWidth + barGap
	seriesCenter := float64(len(maxEntriesValues)-1) / 2

	for index, maxEntries := range maxEntriesValues {
		values := make(plotter.Values, 0, len(writeRatios))

		for _, writeRatio := range writeRatios {
			runs := results[benchmarkKey{maxEntries: maxEntries, writeRatio: writeRatio}]
			if len(runs) == 0 {
				return fmt.Errorf(
					"missing throughput results for max_entries=%d writes=%d",
					maxEntries,
					writeRatio,
				)
			}

			throughput := median(runs) / 1_000_000
			maxThroughput = math.Max(maxThroughput, throughput)
			values = append(values, throughput)
		}

		bars, err := plotter.NewBarChart(values, barWidth)
		if err != nil {
			return fmt.Errorf("create throughput bars: %w", err)
		}

		bars.Color = perfchart.Color(index)
		bars.LineStyle.Width = 0
		bars.Offset = vg.Length((float64(index) - seriesCenter) * float64(seriesStride))

		p.Add(bars)
		p.Legend.Add(perfchart.CountLabel(maxEntries), bars)
	}

	const groupPadding = 0.65

	p.NominalX(writeRatioLabels(writeRatios)...)
	p.X.Min = -groupPadding
	p.X.Max = float64(len(writeRatios)-1) + groupPadding

	setThroughputYRange(p, maxThroughput)

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

		maxEntries, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("parse max entries %q: %w", match[1], err)
		}
		writeRatio, err := strconv.Atoi(match[2])
		if err != nil {
			return nil, fmt.Errorf("parse write ratio %q: %w", match[2], err)
		}
		ops, err := strconv.ParseFloat(match[3], 64)
		if err != nil {
			return nil, fmt.Errorf("parse ops/s %q: %w", match[3], err)
		}

		key := benchmarkKey{maxEntries: maxEntries, writeRatio: writeRatio}
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

func uniqueMaxEntries(results map[benchmarkKey][]float64) []int {
	set := make(map[int]struct{})
	for key := range results {
		set[key.maxEntries] = struct{}{}
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
		set[key.writeRatio] = struct{}{}
	}

	values := make([]int, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Ints(values)
	return values
}

func writeRatioLabels(values []int) []string {
	labels := make([]string, len(values))
	for index, value := range values {
		labels[index] = strconv.Itoa(value)
	}
	return labels
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

func setThroughputYRange(p *plot.Plot, maxValue float64) {
	const tickStep = 10

	padding := math.Max(2, maxValue*0.05)
	maxTick := int(math.Ceil((maxValue+padding)/tickStep) * tickStep)

	p.Y.Min = 0
	p.Y.Max = float64(maxTick)
	p.Y.Tick.Marker = perfchart.IntegerTicks(maxTick, tickStep)
}
