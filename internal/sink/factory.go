package sink

import (
	"fmt"

	"iris/internal/sink/file"
	"iris/internal/sink/kafka"
	"iris/internal/sink/stdout"
	"iris/pkg/cdc"
)

// Config holds sink configuration
type Config struct {
	Type          string
	Brokers       []string            // For Kafka
	TableTopicMap map[string]string   // For Kafka
	Outbox        *kafka.OutboxConfig // For Kafka outbox column shaping (optional)
	Path          string              // For file sink
	MaxSize       int64               // For file sink
	MaxFiles      int                 // For file sink
	PrettyPrint   bool                // For stdout sink
}

// SinkBuilder is a function that creates a sink from config
type SinkBuilder func(cfg Config) (cdc.Sink, error)

// SinkRegistry is a registry-based sink factory
type SinkRegistry struct {
	builders map[string]SinkBuilder
}

// NewFactory creates a new sink factory with registered builders
func NewFactory() *SinkRegistry {
	r := &SinkRegistry{
		builders: make(map[string]SinkBuilder),
	}

	// Register built-in sink types
	r.Register("kafka", buildKafkaSink)
	r.Register("stdout", buildStdoutSink)
	r.Register("file", buildFileSink)

	return r
}

// Register adds a new sink builder to the registry
func (r *SinkRegistry) Register(name string, builder SinkBuilder) {
	r.builders[name] = builder
}

// CreateSink creates a sink instance based on the config type
func (r *SinkRegistry) CreateSink(cfg Config) (cdc.Sink, error) {
	builder, ok := r.builders[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported sink type: %s", cfg.Type)
	}
	return builder(cfg)
}

// buildKafkaSink creates a Kafka sink
func buildKafkaSink(cfg Config) (cdc.Sink, error) {
	return kafka.NewKafkaSink(kafka.Config{
		Brokers:       cfg.Brokers,
		TableTopicMap: cfg.TableTopicMap,
		Outbox:        cfg.Outbox,
	})
}

// buildStdoutSink creates a stdout sink
func buildStdoutSink(cfg Config) (cdc.Sink, error) {
	return stdout.NewStdoutSink(cfg.PrettyPrint), nil
}

// buildFileSink creates a file sink
func buildFileSink(cfg Config) (cdc.Sink, error) {
	return file.NewFileSink(file.Config{
		Path:     cfg.Path,
		MaxSize:  cfg.MaxSize,
		MaxFiles: cfg.MaxFiles,
	})
}
