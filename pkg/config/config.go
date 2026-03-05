package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds the entire pipeline configuration
type Config struct {
	Source    SourceConfig    `yaml:"source"`
	Transform *TransformConfig `yaml:"transform,omitempty"`
	Sink      SinkConfig      `yaml:"sink"`
}

// SourceConfig holds the source database configuration
type SourceConfig struct {
	// Type is the source type (currently only "postgres")
	Type string `yaml:"type"`

	// DSN is the PostgreSQL connection string
	DSN string `yaml:"dsn"`

	// Tables is the list of tables to replicate (empty means all tables)
	Tables []string `yaml:"tables,omitempty"`

	// SlotName is the replication slot name
	SlotName string `yaml:"slot_name"`

	// StartLSN is the WAL position to start from (optional)
	// If empty, starts from current WAL position
	StartLSN string `yaml:"start_lsn,omitempty"`
}

// TransformConfig holds the WASM transform configuration
type TransformConfig struct {
	// Enabled enables the WASM transform
	Enabled bool `yaml:"enabled"`

	// Type is the transform type (currently only "wasm")
	Type string `yaml:"type"`

	// Path is the path to the WASM module file
	Path string `yaml:"path"`

	// FunctionName is the exported function to call (default: "handle")
	FunctionName string `yaml:"function_name,omitempty"`

	// AllocFunctionName is the memory allocation function (default: "alloc")
	AllocFunctionName string `yaml:"alloc_function_name,omitempty"`

	// EnableLogging enables host function logging
	EnableLogging bool `yaml:"enable_logging,omitempty"`
}

// SinkConfig holds the sink configuration
type SinkConfig struct {
	// Type is the sink type (currently only "redis")
	Type string `yaml:"type"`

	// Addr is the Redis server address (e.g., "localhost:6379")
	Addr string `yaml:"addr"`

	// Password is the Redis password (optional)
	Password string `yaml:"password,omitempty"`

	// DB is the Redis database number (default 0)
	DB int `yaml:"db,omitempty"`

	// Key is the Redis list key for CDC events
	Key string `yaml:"key"`

	// MaxLen trims the list to maximum length (0 = no trimming)
	MaxLen int `yaml:"max_len,omitempty"`
}

// Load loads configuration from a YAML file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate source
	if c.Source.Type != "postgres" {
		return fmt.Errorf("unsupported source type: %s", c.Source.Type)
	}
	if c.Source.DSN == "" {
		return fmt.Errorf("source.dsn is required")
	}
	if c.Source.SlotName == "" {
		return fmt.Errorf("source.slot_name is required")
	}

	// Validate transform if enabled
	if c.Transform != nil && c.Transform.Enabled {
		if c.Transform.Type != "wasm" {
			return fmt.Errorf("unsupported transform type: %s", c.Transform.Type)
		}
		if c.Transform.Path == "" {
			return fmt.Errorf("transform.path is required when enabled")
		}
	}

	// Validate sink
	if c.Sink.Type != "redis" {
		return fmt.Errorf("unsupported sink type: %s", c.Sink.Type)
	}
	if c.Sink.Addr == "" {
		return fmt.Errorf("sink.addr is required")
	}
	if c.Sink.Key == "" {
		return fmt.Errorf("sink.key is required")
	}

	return nil
}
