package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"iris/pkg/cdc"

	"github.com/redis/go-redis/v9"
)

// Ensure RedisSink implements cdc.Sink interface
var _ cdc.Sink = (*RedisSink)(nil)

// RedisSink implements Sink interface for Redis list
type RedisSink struct {
	config Config
	client *redis.Client
}

// NewRedisSink creates a new Redis sink with connection testing
func NewRedisSink(cfg Config) (*RedisSink, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redis addr is required")
	}
	if cfg.Key == "" {
		return nil, fmt.Errorf("redis key is required")
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

	return &RedisSink{
		config: cfg,
		client: client,
	}, nil
}

// Write pushes event to Redis list using LPUSH
// Optionally trims the list to MaxLen if configured
func (s *RedisSink) Write(ctx context.Context, event *cdc.Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	// Encode event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	pipe := s.client.Pipeline()
	pipe.LPush(ctx, s.config.Key, data)

	// Optional: trim to max length to prevent unbounded growth
	if s.config.MaxLen > 0 {
		pipe.LTrim(ctx, s.config.Key, 0, int64(s.config.MaxLen-1))
	}

	_, err = pipe.Exec(ctx)
	return err
}

// Close closes the Redis client connection
func (s *RedisSink) Close() error {
	return s.client.Close()
}
