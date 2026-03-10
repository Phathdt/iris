//go:build integration

package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"iris/pkg/cdc"

	redisgo "github.com/redis/go-redis/v9"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// TestIntegration_Factory_CreateRedisSink verifies factory creates working Redis List sink
func TestIntegration_Factory_CreateRedisSink(t *testing.T) {
	ctx := context.Background()

	// Start Redis container
	container, err := rediscontainer.RunContainer(ctx)
	if err != nil {
		t.Fatalf("failed to start Redis container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	// Get address
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	// Create factory and Redis List sink
	factory := NewFactory()
	cfg := Config{
		Type: "redis",
		Addr: addr,
		Key:  "integration:test:factory:list",
	}

	s, err := factory.CreateSink(cfg)
	if err != nil {
		t.Fatalf("CreateSink() error = %v", err)
	}
	defer s.Close()

	// Write event
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		After:  map[string]any{"id": 1},
	}

	if err := s.Write(ctx, event); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify via Redis client
	client := redisgo.NewClient(&redisgo.Options{Addr: addr})
	defer client.Close()

	result, err := client.LRange(ctx, cfg.Key, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange error = %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 item in list, got %d", len(result))
	}

	// Verify JSON
	var decoded cdc.Event
	if err := json.Unmarshal([]byte(result[0]), &decoded); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	if decoded.Table != "users" {
		t.Errorf("Table = %s, want users", decoded.Table)
	}

	// Cleanup
	client.Del(ctx, cfg.Key)
}

// TestIntegration_Factory_CreateRedisStreamSink verifies factory creates working Redis Stream sink
func TestIntegration_Factory_CreateRedisStreamSink(t *testing.T) {
	ctx := context.Background()

	// Start Redis container
	container, err := rediscontainer.RunContainer(ctx)
	if err != nil {
		t.Fatalf("failed to start Redis container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	// Get address
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	// Create factory and Redis Stream sink
	factory := NewFactory()
	streamKey := "integration:test:factory:stream"
	cfg := Config{
		Type: "redis_stream",
		Addr: addr,
		TableStreamMap: map[string]string{
			"users": streamKey,
		},
	}

	s, err := factory.CreateSink(cfg)
	if err != nil {
		t.Fatalf("CreateSink() error = %v", err)
	}
	defer s.Close()

	// Write event
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		After:  map[string]any{"id": 1},
	}

	if err := s.Write(ctx, event); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify via Redis client
	client := redisgo.NewClient(&redisgo.Options{Addr: addr})
	defer client.Close()

	result, err := client.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 entry in stream, got %d", len(result))
	}

	// Verify data field
	entry := result[0]
	dataField, ok := entry.Values["data"].(string)
	if !ok {
		t.Fatal("expected 'data' field in stream entry")
	}

	var decoded cdc.Event
	if err := json.Unmarshal([]byte(dataField), &decoded); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	if decoded.Table != "users" {
		t.Errorf("Table = %s, want users", decoded.Table)
	}

	// Cleanup
	client.Del(ctx, streamKey)
}
