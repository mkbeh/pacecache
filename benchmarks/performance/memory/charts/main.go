package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

	perfchart "github.com/mkbeh/pacecache/benchmarks/performance/internal/chart"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
)

const bytesPerMiB = 1024 * 1024

type result struct {
	capacity   int
	allocBytes uint64
}

func main() {
	input := flag.String(
		"input",
		"./benchmarks/performance/memory/results/memory.txt",
		"path to memory result",
	)
	output := flag.String(
		"output",
		"./benchmarks/performance/memory/assets/memory.png",
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

	points := make(plotter.XYs, len(results))
	capacities := make([]int, len(results))
	for index, result := range results {
		points[index] = plotter.XY{
			X: float64(result.capacity),
			Y: float64(result.allocBytes) / bytesPerMiB,
		}
		capacities[index] = result.capacity
	}

	p := perfchart.New(
		"Memory Consumption",
		"Cache capacity",
		"Live heap (MiB)",
	)
	p.X.Scale = plot.LogScale{}
	p.X.Tick.Marker = perfchart.CapacityTicks(capacities)
	p.X.Min = float64(capacities[0])
	p.X.Max = float64(capacities[len(capacities)-1])
	p.Y.Min = 0

	line, scatter, err := plotter.NewLinePoints(points)
	if err != nil {
		return fmt.Errorf("create memory series: %w", err)
	}

	perfchart.StyleLinePoints(line, scatter, 0)
	p.Add(line, scatter)

	return perfchart.Save(p, output)
}

func readResults(path string) ([]result, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open memory result: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read memory header: %w", err)
	}

	capacityIndex, err := columnIndex(header, "capacity")
	if err != nil {
		return nil, err
	}
	allocIndex, err := columnIndex(header, "alloc_bytes")
	if err != nil {
		return nil, err
	}

	var results []result
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read memory row: %w", err)
		}

		capacity, err := strconv.Atoi(record[capacityIndex])
		if err != nil {
			return nil, fmt.Errorf("parse capacity %q: %w", record[capacityIndex], err)
		}
		allocBytes, err := strconv.ParseUint(record[allocIndex], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse alloc bytes %q: %w", record[allocIndex], err)
		}

		results = append(results, result{
			capacity:   capacity,
			allocBytes: allocBytes,
		})
	}

	if len(results) == 0 {
		return nil, errors.New("no memory rows found")
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].capacity < results[j].capacity
	})

	return results, nil
}

func columnIndex(header []string, name string) (int, error) {
	for index, value := range header {
		if value == name {
			return index, nil
		}
	}
	return 0, fmt.Errorf("missing CSV column %q", name)
}
