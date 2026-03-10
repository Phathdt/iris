//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"iris/pkg/cdc"
)

// TestIntegration_RedisSink_WriteAndRead writes an event to Redis List and verifies via LRANGE
func TestIntegration_RedisSink_WriteAndRead(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	ctx := context.Background()
	cfg := Config{
		Addr: container.Addr(),
		Key:  "integration:test:list:read",
	}

	sink, err := NewRedisSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisSink() error = %v", err)
	}
	defer sink.Close()

	// Write event
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Now(),
		After:  map[string]any{"id": 1, "name": "Alice"},
	}

	if err := sink.Write(ctx, event); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify via LRANGE
	client := container.Client()
	result, err := client.LRange(ctx, cfg.Key, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange error = %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 item in list, got %d", len(result))
	}

	// Verify JSON data roundtrip
	var decoded cdc.Event
	if err := json.Unmarshal([]byte(result[0]), &decoded); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	if decoded.Source != "postgres" {
		t.Errorf("Source = %s, want postgres", decoded.Source)
	}
	if decoded.Table != "users" {
		t.Errorf("Table = %s, want users", decoded.Table)
	}
	if decoded.Op != cdc.EventTypeCreate {
		t.Errorf("Op = %s, want create", decoded.Op)
	}
	if decoded.After == nil {
		t.Error("After is nil, expected data")
	}
}

// TestIntegration_RedisSink_MaxLen writes 5 events with MaxLen=3 and verifies trim
func TestIntegration_RedisSink_MaxLen(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	ctx := context.Background()
	cfg := Config{
		Addr:   container.Addr(),
		Key:    "integration:test:list:maxlen",
		MaxLen: 3,
	}

	sink, err := NewRedisSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisSink() error = %v", err)
	}
	defer sink.Close()

	// Write 5 events
	for i := 0; i < 5; i++ {
		event := &cdc.Event{
			Source: "postgres",
			Table:  "users",
			Op:     cdc.EventTypeCreate,
			After:  map[string]any{"id": i},
		}
		if err := sink.Write(ctx, event); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Verify trimming
	client := container.Client()
	length, err := client.LLen(ctx, cfg.Key).Result()
	if err != nil {
		t.Fatalf("LLen error = %v", err)
	}

	if length != int64(cfg.MaxLen) {
		t.Errorf("list length = %d, want %d", length, cfg.MaxLen)
	}
}

// TestIntegration_RedisSink_ContextCancelled verifies error on cancelled context
func TestIntegration_RedisSink_ContextCancelled(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := Config{
		Addr: container.Addr(),
		Key:  "integration:test:list:cancel",
	}

	sink, err := NewRedisSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisSink() error = %v", err)
	}
	defer sink.Close()

	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
	}

	err = sink.Write(ctx, event)
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}

// TestIntegration_RedisSink_Close verifies Close() is safe and idempotent
func TestIntegration_RedisSink_Close(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	cfg := Config{
		Addr: container.Addr(),
		Key:  "integration:test:list:close",
	}

	sink, err := NewRedisSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisSink() error = %v", err)
	}

	// First close
	if err := sink.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Second close (should be safe)
	if err := sink.Close(); err != nil {
		t.Logf("second Close() returned error (acceptable): %v", err)
	}
}

// TestIntegration_RedisStreamSink_WriteAndRead writes to stream and verifies via XRANGE
func TestIntegration_RedisStreamSink_WriteAndRead(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	ctx := context.Background()
	streamKey := "integration:test:stream:read"
	cfg := StreamConfig{
		Addr: container.Addr(),
		TableStreamMap: map[string]string{
			"users": streamKey,
		},
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisStreamSink() error = %v", err)
	}
	defer sink.Close()

	// Write event
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Now(),
		After:  map[string]any{"id": 1, "name": "Alice"},
	}

	if err := sink.Write(ctx, event); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify via XRANGE
	client := container.Client()
	result, err := client.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 entry in stream, got %d", len(result))
	}

	// Verify JSON data roundtrip
	entry := result[0]
	dataField, ok := entry.Values["data"].(string)
	if !ok {
		t.Fatal("expected 'data' field in stream entry")
	}

	var decoded cdc.Event
	if err := json.Unmarshal([]byte(dataField), &decoded); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	if decoded.Source != "postgres" {
		t.Errorf("Source = %s, want postgres", decoded.Source)
	}
	if decoded.Table != "users" {
		t.Errorf("Table = %s, want users", decoded.Table)
	}
	if decoded.Op != cdc.EventTypeCreate {
		t.Errorf("Op = %s, want create", decoded.Op)
	}
}

// TestIntegration_RedisStreamSink_MaxLen writes 5 events with MaxLen=3 and verifies exact trim
func TestIntegration_RedisStreamSink_MaxLen(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	ctx := context.Background()
	streamKey := "integration:test:stream:maxlen"
	cfg := StreamConfig{
		Addr: container.Addr(),
		TableStreamMap: map[string]string{
			"users": streamKey,
		},
		MaxLen: 3,
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisStreamSink() error = %v", err)
	}
	defer sink.Close()

	// Write 5 events
	for i := 0; i < 5; i++ {
		event := &cdc.Event{
			Source: "postgres",
			Table:  "users",
			Op:     cdc.EventTypeCreate,
			After:  map[string]any{"id": i},
		}
		if err := sink.Write(ctx, event); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Verify exact trimming
	client := container.Client()
	result, err := client.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	if len(result) != cfg.MaxLen {
		t.Errorf("stream length = %d, want exactly %d", len(result), cfg.MaxLen)
	}
}

// TestIntegration_RedisStreamSink_MaxLenApprox tests approximate trim with tolerance
func TestIntegration_RedisStreamSink_MaxLenApprox(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	ctx := context.Background()
	streamKey := "integration:test:stream:maxlenapprox"
	cfg := StreamConfig{
		Addr: container.Addr(),
		TableStreamMap: map[string]string{
			"users": streamKey,
		},
		MaxLen:          3,
		ApproximateTrim: true,
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisStreamSink() error = %v", err)
	}
	defer sink.Close()

	// Write 5 events
	for i := 0; i < 5; i++ {
		event := &cdc.Event{
			Source: "postgres",
			Table:  "users",
			Op:     cdc.EventTypeCreate,
			After:  map[string]any{"id": i},
		}
		if err := sink.Write(ctx, event); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Verify approximate trimming (within tolerance)
	client := container.Client()
	result, err := client.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	// With approximate trimming, length should be around MaxLen with some tolerance
	if len(result) < cfg.MaxLen-1 || len(result) > cfg.MaxLen+2 {
		t.Errorf("stream length = %d, expected around %d (within +/-2)", len(result), cfg.MaxLen)
	}
}

// TestIntegration_RedisStreamSink_TableRouting verifies multiple tables route to different stream keys
func TestIntegration_RedisStreamSink_TableRouting(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	ctx := context.Background()
	usersStream := "integration:test:stream:users"
	ordersStream := "integration:test:stream:orders"

	cfg := StreamConfig{
		Addr: container.Addr(),
		TableStreamMap: map[string]string{
			"users":  usersStream,
			"orders": ordersStream,
		},
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisStreamSink() error = %v", err)
	}
	defer sink.Close()

	// Write events to different tables
	events := []*cdc.Event{
		{Source: "postgres", Table: "users", Op: cdc.EventTypeCreate},
		{Source: "postgres", Table: "users", Op: cdc.EventTypeUpdate},
		{Source: "postgres", Table: "orders", Op: cdc.EventTypeCreate},
		{Source: "postgres", Table: "orders", Op: cdc.EventTypeDelete},
	}

	for _, event := range events {
		if err := sink.Write(ctx, event); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Verify routing
	client := container.Client()
	usersResult, err := client.XRange(ctx, usersStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange users error = %v", err)
	}

	ordersResult, err := client.XRange(ctx, ordersStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange orders error = %v", err)
	}

	if len(usersResult) != 2 {
		t.Errorf("users stream has %d events, want 2", len(usersResult))
	}
	if len(ordersResult) != 2 {
		t.Errorf("orders stream has %d events, want 2", len(ordersResult))
	}

	// Verify content
	for i, expected := range []struct {
		op cdc.EventType
	}{{cdc.EventTypeCreate}, {cdc.EventTypeUpdate}} {
		dataField, ok := usersResult[i].Values["data"].(string)
		if !ok {
			t.Errorf("users[%d]: missing 'data' field", i)
			continue
		}
		var decoded cdc.Event
		if err := json.Unmarshal([]byte(dataField), &decoded); err != nil {
			t.Errorf("users[%d]: unmarshal error = %v", i, err)
			continue
		}
		if decoded.Op != expected.op {
			t.Errorf("users[%d]: Op = %s, want %s", i, decoded.Op, expected.op)
		}
	}
}

// TestIntegration_RedisStreamSink_DefaultStreamKey tests default "cdc:{table}" routing
func TestIntegration_RedisStreamSink_DefaultStreamKey(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	ctx := context.Background()

	// No TableStreamMap - should use defaults
	cfg := StreamConfig{
		Addr:           container.Addr(),
		TableStreamMap: map[string]string{},
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisStreamSink() error = %v", err)
	}
	defer sink.Close()

	// Write event without explicit mapping
	event := &cdc.Event{
		Source: "postgres",
		Table:  "products",
		Op:     cdc.EventTypeCreate,
	}

	if err := sink.Write(ctx, event); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify it uses default key "cdc:products"
	client := container.Client()
	result, err := client.XRange(ctx, "cdc:products", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 entry in default stream, got %d", len(result))
	}
}

// TestIntegration_RedisStreamSink_ContextCancelled verifies error on cancelled context
func TestIntegration_RedisStreamSink_ContextCancelled(t *testing.T) {
	container := SetupRedisContainer(t)
	defer container.FlushDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := StreamConfig{
		Addr: container.Addr(),
		TableStreamMap: map[string]string{
			"users": "integration:test:stream:cancel",
		},
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Fatalf("NewRedisStreamSink() error = %v", err)
	}
	defer sink.Close()

	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
	}

	err = sink.Write(ctx, event)
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}

