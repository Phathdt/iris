package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisSink implements Sink interface for Redis list
type RedisSink struct {
	config Config
	client *redis.Client
}

// NewRedisSink creates a new Redis sink with connection testing
func NewRedisSink(cfg Config) (*RedisSink, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Test connection on initialization
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &RedisSink{
		config: cfg,
		client: client,
	}, nil
}

// Write pushes encoded event to Redis list using LPUSH
// Optionally trims the list to MaxLen if configured
func (s *RedisSink) Write(ctx context.Context, data []byte) error {
	pipe := s.client.Pipeline()
	pipe.LPush(ctx, s.config.Key, data)

	// Optional: trim to max length to prevent unbounded growth
	if s.config.MaxLen > 0 {
		pipe.LTrim(ctx, s.config.Key, 0, int64(s.config.MaxLen-1))
	}

	_, err := pipe.Exec(ctx)
	return err
}

// Close closes the Redis client connection
func (s *RedisSink) Close() error {
	return s.client.Close()
}
