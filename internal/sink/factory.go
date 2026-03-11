package sink

import (
	"fmt"

	"iris/internal/sink/kafka"
	"iris/internal/sink/redis"
	"iris/pkg/cdc"
)

// Config holds sink configuration
type Config struct {
	Type            string
	Addr            string            // For Redis
	Password        string            // For Redis
	DB              int               // For Redis
	Key             string            // For Redis List
	TableStreamMap  map[string]string // For Redis Stream
	MaxLen          int               // For Redis
	ApproximateTrim bool              // For Redis Stream
	Brokers         []string          // For Kafka
	TableTopicMap   map[string]string // For Kafka
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
	r.Register("redis", buildRedisListSink)
	r.Register("redis_stream", buildRedisStreamSink)
	r.Register("kafka", buildKafkaSink)

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

// buildRedisListSink creates a Redis List sink
func buildRedisListSink(cfg Config) (cdc.Sink, error) {
	return redis.NewRedisSink(redis.Config{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		Key:      cfg.Key,
		MaxLen:   cfg.MaxLen,
	})
}

// buildRedisStreamSink creates a Redis Stream sink
func buildRedisStreamSink(cfg Config) (cdc.Sink, error) {
	return redis.NewRedisStreamSink(redis.StreamConfig{
		Addr:            cfg.Addr,
		Password:        cfg.Password,
		DB:              cfg.DB,
		TableStreamMap:  cfg.TableStreamMap,
		MaxLen:          cfg.MaxLen,
		ApproximateTrim: cfg.ApproximateTrim,
	})
}

// buildKafkaSink creates a Kafka sink
func buildKafkaSink(cfg Config) (cdc.Sink, error) {
	return kafka.NewKafkaSink(kafka.Config{
		Brokers:       cfg.Brokers,
		TableTopicMap: cfg.TableTopicMap,
	})
}
