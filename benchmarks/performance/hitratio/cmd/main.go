package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/mkbeh/pacecache/benchmarks/performance/hitratio/internal/config"
	"github.com/mkbeh/pacecache/benchmarks/performance/hitratio/internal/simulator"
)

func main() {
	var configPath string

	flag.StringVar(
		&configPath,
		"config",
		"hitratio/configs/zipf.toml",
		"path to configuration file",
	)

	flag.Parse()

	if err := run(configPath); err != nil {
		log.Fatal(err)
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	app := simulator.New(cfg)

	if err := app.Simulate(os.Stdout); err != nil {
		return fmt.Errorf("simulate trace: %w", err)
	}

	return nil
}
