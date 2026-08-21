package chart

import (
	"fmt"
	"image/color"
	"os"
	"path/filepath"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
)

const (
	plotWidth  = 12.5 * vg.Inch
	plotHeight = 7.03125 * vg.Inch
)

var palette = []color.Color{
	color.NRGBA{R: 0x2F, G: 0x6F, B: 0xD0, A: 0xFF},
	color.NRGBA{R: 0xE6, G: 0x7E, B: 0x22, A: 0xFF},
	color.NRGBA{R: 0x2E, G: 0xA0, B: 0x5B, A: 0xFF},
	color.NRGBA{R: 0x8E, G: 0x5C, B: 0xC7, A: 0xFF},
}

func New(title, xLabel, yLabel string) *plot.Plot {
	p := plot.New()
	p.Title.Text = title
	p.Title.Padding = vg.Points(10)
	p.X.Label.Text = xLabel
	p.Y.Label.Text = yLabel

	applyTypography(p)

	p.Legend.Top = true
	p.Legend.Left = false
	p.Legend.Padding = vg.Points(5)
	p.Legend.ThumbnailWidth = vg.Points(24)
	p.Legend.XOffs = -vg.Points(10)
	p.Legend.YOffs = -vg.Points(10)

	grid := plotter.NewGrid()
	gridColor := color.NRGBA{R: 0xE3, G: 0xE7, B: 0xEB, A: 0xFF}
	grid.Vertical.Color = gridColor
	grid.Horizontal.Color = gridColor
	grid.Vertical.Width = vg.Points(0.6)
	grid.Horizontal.Width = vg.Points(0.6)
	p.Add(grid)

	return p
}

func applyTypography(p *plot.Plot) {
	const variant = "Sans"

	p.Title.TextStyle.Font.Variant = variant
	p.Title.TextStyle.Font.Size = vg.Points(18)

	p.X.Label.TextStyle.Font.Variant = variant
	p.X.Label.TextStyle.Font.Size = vg.Points(13)
	p.X.Label.Padding = vg.Points(8)
	p.Y.Label.TextStyle.Font.Variant = variant
	p.Y.Label.TextStyle.Font.Size = vg.Points(13)
	p.Y.Label.Padding = vg.Points(8)

	p.X.Tick.Label.Font.Variant = variant
	p.X.Tick.Label.Font.Size = vg.Points(11)
	p.Y.Tick.Label.Font.Variant = variant
	p.Y.Tick.Label.Font.Size = vg.Points(11)

	p.Legend.TextStyle.Font.Variant = variant
	p.Legend.TextStyle.Font.Size = vg.Points(11)
}

func StyleLinePoints(line *plotter.Line, scatter *plotter.Scatter, index int) {
	seriesColor := Color(index)
	line.Color = seriesColor
	line.Width = vg.Points(2.5)
	scatter.Color = seriesColor
	scatter.Shape = draw.CircleGlyph{}
	scatter.Radius = vg.Points(4.25)
}

func Color(index int) color.Color {
	return palette[index%len(palette)]
}

func CapacityLabel(value int) string {
	switch {
	case value >= 1_000_000 && value%1_000_000 == 0:
		return fmt.Sprintf("%dM", value/1_000_000)
	case value >= 1_000 && value%1_000 == 0:
		return fmt.Sprintf("%dK", value/1_000)
	default:
		return fmt.Sprintf("%d", value)
	}
}

func CapacityTicks(values []int) plot.ConstantTicks {
	ticks := make(plot.ConstantTicks, 0, len(values))
	for _, value := range values {
		ticks = append(ticks, plot.Tick{
			Value: float64(value),
			Label: CapacityLabel(value),
		})
	}
	return ticks
}

func Save(p *plot.Plot, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := p.Save(plotWidth, plotHeight, path); err != nil {
		return fmt.Errorf("save plot: %w", err)
	}
	return nil
}
