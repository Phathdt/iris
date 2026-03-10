//go:build integration

package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// RedisContainer wraps the testcontainer and provides convenient helpers
type RedisContainer struct {
	container *rediscontainer.RedisContainer
	client    *redis.Client
	addr      string
}

// SetupRedisContainer starts a Redis container and returns a RedisContainer helper
// It uses t.Cleanup() to ensure the container is stopped after the test completes
func SetupRedisContainer(t *testing.T) *RedisContainer {
	ctx := context.Background()

	// Start Redis container (7-alpine)
	container, err := rediscontainer.RunContainer(ctx)
	if err != nil {
		t.Fatalf("failed to start Redis container: %v", err)
	}

	// Register cleanup
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate Redis container: %v", err)
		}
	})

	// Get the host:port address
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to ping Redis container: %v", err)
	}

	return &RedisContainer{
		container: container,
		client:    client,
		addr:      addr,
	}
}

// Addr returns the container address (host:port)
func (rc *RedisContainer) Addr() string {
	return rc.addr
}

// Client returns the Redis client
func (rc *RedisContainer) Client() *redis.Client {
	return rc.client
}

// FlushDB flushes all databases in the container
func (rc *RedisContainer) FlushDB(t *testing.T) {
	ctx := context.Background()
	if err := rc.client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to flush Redis DB: %v", err)
	}
}

// DeleteKeys deletes specified keys from Redis
func (rc *RedisContainer) DeleteKeys(t *testing.T, keys ...string) {
	ctx := context.Background()
	if err := rc.client.Del(ctx, keys...).Err(); err != nil {
		t.Logf("failed to delete Redis keys: %v", err)
	}
}
