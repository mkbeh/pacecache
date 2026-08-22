package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"

	perfchart "github.com/mkbeh/pacecache/benchmarks/performance/internal/chart"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
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

	values := make(plotter.Values, len(results))
	labels := make([]string, len(results))
	labelPoints := make(plotter.XYs, len(results))
	valueLabels := make([]string, len(results))

	var maxHeap float64

	for index, result := range results {
		heap := float64(result.allocBytes) / bytesPerMiB

		values[index] = heap
		labels[index] = perfchart.CountLabel(result.capacity)
		labelPoints[index] = plotter.XY{
			X: float64(index),
			Y: heap,
		}
		valueLabels[index] = memoryLabel(heap)
		maxHeap = math.Max(maxHeap, heap)
	}

	p := perfchart.New(
		"Memory Usage by Cache Capacity",
		"Cache capacity",
		"Live heap (MiB)",
	)

	const barWidth = 80

	bars, err := plotter.NewBarChart(values, vg.Points(barWidth))
	if err != nil {
		return fmt.Errorf("create memory bars: %w", err)
	}

	bars.Color = perfchart.Color(0)
	bars.LineStyle.Width = 0
	p.Add(bars)

	valueLabelPlot, err := plotter.NewLabels(plotter.XYLabels{
		XYs:    labelPoints,
		Labels: valueLabels,
	})
	if err != nil {
		return fmt.Errorf("create memory labels: %w", err)
	}

	valueLabelPlot.Offset = vg.Point{Y: vg.Points(6)}
	for index := range valueLabelPlot.TextStyle {
		valueLabelPlot.TextStyle[index].Font.Variant = "Sans"
		valueLabelPlot.TextStyle[index].Font.Size = vg.Points(10)
		valueLabelPlot.TextStyle[index].XAlign = draw.XCenter
		valueLabelPlot.TextStyle[index].YAlign = draw.YBottom
	}
	p.Add(valueLabelPlot)

	const groupPadding = 0.55

	p.NominalX(labels...)
	p.X.Min = -groupPadding
	p.X.Max = float64(len(results)-1) + groupPadding

	setMemoryYRange(p, maxHeap)

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

func memoryLabel(value float64) string {
	if value >= 10 {
		return fmt.Sprintf("%.1f MiB", value)
	}

	return fmt.Sprintf("%.2f MiB", value)
}

func setMemoryYRange(p *plot.Plot, maxValue float64) {
	const tickStep = 20

	padding := math.Max(2, maxValue*0.05)
	maxTick := int(math.Ceil((maxValue+padding)/tickStep) * tickStep)

	p.Y.Min = 0
	p.Y.Max = float64(maxTick)
	p.Y.Tick.Marker = perfchart.IntegerTicks(maxTick, tickStep)
}

func columnIndex(header []string, name string) (int, error) {
	for index, value := range header {
		if value == name {
			return index, nil
		}
	}
	return 0, fmt.Errorf("missing CSV column %q", name)
}
