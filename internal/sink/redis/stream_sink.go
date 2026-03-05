package redis

import (
	"context"
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
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Test connection on initialization
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &RedisStreamSink{
		config: cfg,
		client: client,
	}, nil
}

// Write pushes encoded event data to Redis Stream using XADD
// Optionally trims the stream to MaxLen if configured
func (s *RedisStreamSink) Write(ctx context.Context, data []byte) error {
	// Build field map for XADD
	// Store the JSON payload in a 'data' field, plus metadata
	fields := map[string]any{
		"data": string(data),
	}

	// Build XADD args
	args := &redis.XAddArgs{
		Stream: s.config.StreamKey,
		ID:     "*", // Auto-generate stream ID
		Values: fields,
	}

	// Execute XADD
	if err := s.client.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("xadd: %w", err)
	}

	// Optional: trim stream to max length
	if s.config.MaxLen > 0 {
		var err error
		if s.config.ApproximateTrim {
			// Use approximate trimming for better performance
			err = s.client.XTrimMaxLenApprox(ctx, s.config.StreamKey, int64(s.config.MaxLen), 0).Err()
		} else {
			// Exact trimming
			err = s.client.XTrimMaxLen(ctx, s.config.StreamKey, int64(s.config.MaxLen)).Err()
		}
		// Note: Trim errors are intentionally ignored to avoid failing the write.
		// The event was already added successfully via XADD above.
		// Trimming is a best-effort operation for retention policy, not critical path.
		_ = err
	}

	return nil
}

// Close closes the Redis client connection
func (s *RedisStreamSink) Close() error {
	return s.client.Close()
}
