package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"iris/pkg/cdc"

	"github.com/redis/go-redis/v9"
)

// Ensure RedisStreamSink implements cdc.Sink interface
var _ cdc.Sink = (*RedisStreamSink)(nil)

// RedisStreamSink implements Sink interface for Redis Stream
type RedisStreamSink struct {
	config StreamConfig
	client *redis.Client
}

// NewRedisStreamSink creates a new Redis Stream sink with connection testing
func NewRedisStreamSink(cfg StreamConfig) (*RedisStreamSink, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redis addr is required")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Test connection on initialization
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &RedisStreamSink{
		config: cfg,
		client: client,
	}, nil
}

// Write pushes event to Redis Stream using XADD
// The stream key is determined by config.GetStreamKey(event.Table)
// Optionally trims the stream to MaxLen if configured
func (s *RedisStreamSink) Write(ctx context.Context, event *cdc.Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	// Determine stream key for this table
	streamKey := s.config.GetStreamKey(event.Table)

	// Encode event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	// Build field map for XADD
	// Store the JSON payload in a 'data' field
	fields := map[string]any{
		"data": string(data),
	}

	// Build XADD args with inline trimming
	args := &redis.XAddArgs{
		Stream: streamKey,
		ID:     "*", // Auto-generate stream ID
		Values: fields,
	}

	// Atomic trim within XADD if MaxLen is configured
	if s.config.MaxLen > 0 {
		args.MaxLen = int64(s.config.MaxLen)
		args.Approx = s.config.ApproximateTrim
	}

	if err := s.client.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("xadd: %w", err)
	}

	return nil
}

// Close closes the Redis client connection
func (s *RedisStreamSink) Close() error {
	return s.client.Close()
}
