package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Version info set at build time via ldflags
var (
	version   = "dev"
	gitCommit = "none"
	buildDate = "unknown"
)

// Version returns the current version
func Version() string {
	return version
}

// PrintVersion prints version information
func PrintVersion() {
	fmt.Printf("iris version %s (commit: %s, built: %s)\n", version, gitCommit, buildDate)
}

// Config holds the entire pipeline configuration
type Config struct {
	Source        SourceConfig        `yaml:"source"`
	Transform     *TransformConfig    `yaml:"transform,omitempty"`
	Sink          SinkConfig          `yaml:"sink"`
	Mapping       MappingConfig       `yaml:"mapping,omitempty"`
	Logger        LoggerConfig        `yaml:"logger,omitempty"`
	DLQ           *DLQConfig          `yaml:"dlq,omitempty"`
	Retry         RetryConfig         `yaml:"retry,omitempty"`
	Observability ObservabilityConfig `yaml:"observability,omitempty"`
}

// ObservabilityConfig holds metrics and tracing configuration
type ObservabilityConfig struct {
	Metrics MetricsConfig `yaml:"metrics,omitempty"`
	Tracing TracingConfig `yaml:"tracing,omitempty"`
}

// MetricsConfig holds Prometheus metrics server configuration
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port,omitempty"`
	Bind    string `yaml:"bind,omitempty"`
}

// TracingConfig holds OpenTelemetry tracing configuration
type TracingConfig struct {
	Enabled     bool    `yaml:"enabled"`
	Endpoint    string  `yaml:"endpoint,omitempty"`
	ServiceName string  `yaml:"service_name,omitempty"`
	SampleRate  float64 `yaml:"sample_rate,omitempty"`
}

// DLQConfig holds the dead letter queue configuration
type DLQConfig struct {
	// Enabled enables the DLQ sink for failed events
	Enabled bool       `yaml:"enabled"`
	Sink    SinkConfig `yaml:"sink"`
}

// RetryConfig holds retry settings for transform and sink operations
type RetryConfig struct {
	// MaxAttempts is the maximum number of attempts before sending to DLQ (default: 3)
	MaxAttempts int `yaml:"max_attempts,omitempty"`
	// BackoffMs is the delay between retries in milliseconds (default: 100)
	BackoffMs int `yaml:"backoff_ms,omitempty"`
}

// MappingConfig holds table-to-stream routing configuration
type MappingConfig struct {
	// TableStreamMap maps table names to stream keys
	// If not set, defaults to "cdc:{table}"
	TableStreamMap map[string]string `yaml:"table_stream_map,omitempty"`
}

// LoggerConfig holds the logger configuration
type LoggerConfig struct {
	// Level is the logging level (debug, info, warn, error)
	Level string `yaml:"level"`

	// Format is the output format (json, text, plain)
	Format string `yaml:"format"`
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

// TransformModuleConfig holds configuration for a single WASM transform module
type TransformModuleConfig struct {
	// Path is the path to the WASM module file
	Path string `yaml:"path"`

	// FunctionName is the exported function to call (default: "handle")
	FunctionName string `yaml:"function_name,omitempty"`

	// AllocFunctionName is the memory allocation function (default: "alloc")
	AllocFunctionName string `yaml:"alloc_function_name,omitempty"`

	// EnableLogging enables host function logging
	EnableLogging bool `yaml:"enable_logging,omitempty"`
}

// TransformConfig holds the WASM transform configuration
type TransformConfig struct {
	// Enabled enables the WASM transform
	Enabled bool `yaml:"enabled"`

	// Type is the transform type (currently only "wasm")
	Type string `yaml:"type"`

	// Modules is the list of WASM modules to apply sequentially
	// If specified, transforms are applied in order (chain)
	Modules []TransformModuleConfig `yaml:"modules,omitempty"`

	// Path is the path to a single WASM module file (for backward compatibility)
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
	// Type is the sink type ("redis" for list, "redis_stream" for stream)
	Type string `yaml:"type"`

	// Addr is the Redis server address (e.g., "localhost:6379")
	Addr string `yaml:"addr"`

	// Password is the Redis password (optional)
	Password string `yaml:"password,omitempty"`

	// DB is the Redis database number (default 0)
	DB int `yaml:"db,omitempty"`

	// Key is the Redis list key for CDC events (used when type="redis")
	Key string `yaml:"key,omitempty"`

	// MaxLen trims the list/stream to maximum length (0 = no trimming)
	MaxLen int `yaml:"max_len,omitempty"`

	// ApproximateTrim uses ~MAXLEN for better performance (default: false)
	// Only used when type="redis_stream"
	ApproximateTrim bool `yaml:"approximate_trim,omitempty"`

	// Brokers is the list of Kafka broker addresses (used when type="kafka")
	Brokers []string `yaml:"brokers,omitempty"`
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

	// Set default logger config if not specified
	if cfg.Logger.Level == "" {
		cfg.Logger.Level = "info"
	}
	if cfg.Logger.Format == "" {
		cfg.Logger.Format = "text"
	}

	// Set default retry config
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry.MaxAttempts = 3
	}
	if cfg.Retry.BackoffMs <= 0 {
		cfg.Retry.BackoffMs = 100
	}

	// Set default observability config
	if cfg.Observability.Metrics.Port <= 0 {
		cfg.Observability.Metrics.Port = 9090
	}
	if cfg.Observability.Metrics.Bind == "" {
		cfg.Observability.Metrics.Bind = "0.0.0.0"
	}
	if cfg.Observability.Tracing.Endpoint == "" {
		cfg.Observability.Tracing.Endpoint = "localhost:4317"
	}
	if cfg.Observability.Tracing.ServiceName == "" {
		cfg.Observability.Tracing.ServiceName = "iris"
	}
	if cfg.Observability.Tracing.SampleRate <= 0 {
		cfg.Observability.Tracing.SampleRate = 1.0
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
		// Support both single Path (backward compatibility) and Modules array
		hasPath := c.Transform.Path != ""
		hasModules := len(c.Transform.Modules) > 0

		if !hasPath && !hasModules {
			return fmt.Errorf("transform.path or transform.modules is required when enabled")
		}

		// Validate each module in the chain
		for i, mod := range c.Transform.Modules {
			if mod.Path == "" {
				return fmt.Errorf("transform.modules[%d].path is required", i)
			}
		}
	}

	// Validate sink (per-type validation)
	if err := validateSink(c.Sink, "sink"); err != nil {
		return err
	}

	// Validate DLQ sink if enabled
	if c.DLQ != nil && c.DLQ.Enabled {
		if err := validateSink(c.DLQ.Sink, "dlq.sink"); err != nil {
			return err
		}
	}

	return nil
}

// validateSink validates a sink configuration with a given prefix for error messages
func validateSink(s SinkConfig, prefix string) error {
	switch s.Type {
	case "redis":
		if s.Addr == "" {
			return fmt.Errorf("%s.addr is required for redis sink", prefix)
		}
		if s.Key == "" {
			return fmt.Errorf("%s.key is required for redis list sink", prefix)
		}
	case "redis_stream":
		if s.Addr == "" {
			return fmt.Errorf("%s.addr is required for redis_stream sink", prefix)
		}
	case "kafka":
		if len(s.Brokers) == 0 {
			return fmt.Errorf("%s.brokers is required for kafka sink", prefix)
		}
	default:
		return fmt.Errorf("unsupported %s type: %s", prefix, s.Type)
	}
	return nil
}
