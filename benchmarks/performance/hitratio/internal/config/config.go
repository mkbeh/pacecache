package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

const ZipfType = "zipf"

type Config struct {
	Type       string `toml:"type"`
	Name       string `toml:"name"`
	Capacities []uint `toml:"capacities"`
	Segments   int    `toml:"segments"`
	Limit      *uint  `toml:"limit"`
	Zipf       *Zipf  `toml:"zipf"`
}

type Zipf struct {
	S    float64 `toml:"s"`
	V    float64 `toml:"v"`
	IMAX uint64  `toml:"imax"`
}

func Load(path string) (Config, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := toml.Unmarshal(content, &cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func (cfg Config) validate() error {
	if cfg.Type != ZipfType {
		return fmt.Errorf("unsupported trace type %q", cfg.Type)
	}

	if cfg.Name == "" {
		return errors.New("name is empty")
	}

	if len(cfg.Capacities) == 0 {
		return errors.New("capacities is empty")
	}

	for _, capacity := range cfg.Capacities {
		if capacity == 0 {
			return errors.New("capacity must be greater than zero")
		}
	}

	if cfg.Segments <= 0 {
		return errors.New("segments must be greater than zero")
	}

	if cfg.Limit == nil {
		return errors.New("zipf trace must have a limit")
	}

	if *cfg.Limit == 0 {
		return errors.New("limit must be greater than zero")
	}

	if cfg.Zipf == nil {
		return errors.New("zipf parameters are missing")
	}

	if cfg.Zipf.S <= 1 {
		return errors.New("zipf s must be greater than 1")
	}

	if cfg.Zipf.V < 1 {
		return errors.New("zipf v must be at least 1")
	}

	return nil
}
