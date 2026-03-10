package redis

import (
	"context"
	"encoding/json"
	"testing"

	"iris/pkg/cdc"

	"github.com/redis/go-redis/v9"
)

func TestStreamConfig_GetStreamKey(t *testing.T) {
	tests := []struct {
		name     string
		cfg      StreamConfig
		table    string
		expected string
	}{
		{
			name:     "explicit mapping",
			cfg:      StreamConfig{TableStreamMap: map[string]string{"users": "my:users:stream"}},
			table:    "users",
			expected: "my:users:stream",
		},
		{
			name:     "default when no mapping",
			cfg:      StreamConfig{},
			table:    "orders",
			expected: "cdc:orders",
		},
		{
			name:     "default for unmapped table",
			cfg:      StreamConfig{TableStreamMap: map[string]string{"users": "my:stream"}},
			table:    "orders",
			expected: "cdc:orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.GetStreamKey(tt.table)
			if got != tt.expected {
				t.Errorf("GetStreamKey(%q) = %q, want %q", tt.table, got, tt.expected)
			}
		})
	}
}

func TestStreamConfig_Defaults(t *testing.T) {
	cfg := StreamConfig{
		Addr: "localhost:6379",
		TableStreamMap: map[string]string{
			"test_table": "test:stream",
		},
	}

	if cfg.DB != 0 {
		t.Errorf("default DB should be 0, got %d", cfg.DB)
	}
	if cfg.MaxLen != 0 {
		t.Errorf("default MaxLen should be 0, got %d", cfg.MaxLen)
	}
	if cfg.ApproximateTrim {
		t.Errorf("default ApproximateTrim should be false")
	}
}

func TestRedisStreamSink_NewRedisStreamSink_InvalidAddr(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Test with invalid address
	cfg := StreamConfig{
		Addr: "invalid-address:99999",
		TableStreamMap: map[string]string{
			"test_table": "test:stream",
		},
	}

	_, err := NewRedisStreamSink(cfg)
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisStreamSink_NewRedisStreamSink_ConnectionError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Test with unreachable address
	cfg := StreamConfig{
		Addr: "localhost:1", // Port 1 is typically closed
		TableStreamMap: map[string]string{
			"test_table": "test:stream",
		},
	}

	_, err := NewRedisStreamSink(cfg)
	if err == nil {
		t.Fatal("expected error for unreachable Redis")
	}
}

func TestRedisStreamSink_Write(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// This test requires a running Redis instance
	ctx := context.Background()

	cfg := StreamConfig{
		Addr: "localhost:6379",
		TableStreamMap: map[string]string{
			"users": "test:cdc:stream:unit",
		},
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     "create",
		After:  map[string]any{"id": 1, "name": "test"},
	}

	err = sink.Write(ctx, event)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify data was written
	client := redis.NewClient(&redis.Options{
		Addr: cfg.Addr,
	})
	defer client.Close()

	result, err := client.XRange(ctx, "test:cdc:stream:unit", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	if len(result) == 0 {
		t.Fatal("expected data in Redis stream")
	}

	// Check the 'data' field
	entry := result[0]
	dataField, ok := entry.Values["data"].(string)
	if !ok {
		t.Fatal("expected 'data' field in stream entry")
	}

	// Verify the JSON data
	var decoded cdc.Event
	if err := json.Unmarshal([]byte(dataField), &decoded); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}
	if decoded.Table != "users" {
		t.Errorf("expected table=users, got %s", decoded.Table)
	}

	// Cleanup
	client.Del(ctx, "test:cdc:stream:unit")
}

func TestRedisStreamSink_Write_MaxLen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	cfg := StreamConfig{
		Addr: "localhost:6379",
		TableStreamMap: map[string]string{
			"users": "test:cdc:stream:maxlen",
		},
		MaxLen: 3,
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	// Write more items than MaxLen
	for i := 0; i < 5; i++ {
		event := &cdc.Event{
			Source: "postgres",
			Table:  "users",
			Op:     "create",
			After:  map[string]any{"id": i},
		}
		if err := sink.Write(ctx, event); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Verify stream is trimmed to MaxLen
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
	defer client.Close()

	result, err := client.XRange(ctx, "test:cdc:stream:maxlen", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	if len(result) != cfg.MaxLen {
		t.Errorf("stream length = %d, want %d", len(result), cfg.MaxLen)
	}

	client.Del(ctx, "test:cdc:stream:maxlen")
}

func TestRedisStreamSink_Write_MaxLenApprox(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	cfg := StreamConfig{
		Addr: "localhost:6379",
		TableStreamMap: map[string]string{
			"users": "test:cdc:stream:maxlenapprox",
		},
		MaxLen:          3,
		ApproximateTrim: true,
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	// Write more items than MaxLen
	for i := 0; i < 5; i++ {
		event := &cdc.Event{
			Source: "postgres",
			Table:  "users",
			Op:     "create",
			After:  map[string]any{"id": i},
		}
		if err := sink.Write(ctx, event); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Verify stream is trimmed (approximately) to MaxLen
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
	defer client.Close()

	result, err := client.XRange(ctx, "test:cdc:stream:maxlenapprox", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	// With approximate trimming, length should be around MaxLen
	if len(result) < cfg.MaxLen-1 || len(result) > cfg.MaxLen+2 {
		t.Errorf("stream length = %d, expected around %d", len(result), cfg.MaxLen)
	}

	client.Del(ctx, "test:cdc:stream:maxlenapprox")
}

func TestRedisStreamSink_Write_ContextCancelled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	cfg := StreamConfig{
		Addr: "localhost:6379",
		TableStreamMap: map[string]string{
			"users": "test:stream",
		},
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	err = sink.Write(ctx, &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     "create",
	})
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestRedisStreamSink_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cfg := StreamConfig{
		Addr: "localhost:6379",
		TableStreamMap: map[string]string{
			"users": "test:stream",
		},
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	err = sink.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Second close should be safe
	err = sink.Close()
	// May or may not error depending on implementation
	_ = err
}

func TestRedisStreamSink_Integration_FullFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	usersStream := "test:cdc:fullflow:users"
	ordersStream := "test:cdc:fullflow:orders"

	cfg := StreamConfig{
		Addr: "localhost:6379",
		TableStreamMap: map[string]string{
			"users":  usersStream,
			"orders": ordersStream,
		},
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	// Write multiple events across different tables
	events := []*cdc.Event{
		{Source: "postgres", Table: "users", Op: "create"},
		{Source: "postgres", Table: "users", Op: "update"},
		{Source: "postgres", Table: "orders", Op: "delete"},
	}

	for _, event := range events {
		if err := sink.Write(ctx, event); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Read and verify per-table routing
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
	defer client.Close()

	// Verify users stream has 2 events
	usersResult, err := client.XRange(ctx, usersStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange users error = %v", err)
	}
	if len(usersResult) != 2 {
		t.Fatalf("users stream: expected 2 events, got %d", len(usersResult))
	}

	// Verify orders stream has 1 event
	ordersResult, err := client.XRange(ctx, ordersStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange orders error = %v", err)
	}
	if len(ordersResult) != 1 {
		t.Fatalf("orders stream: expected 1 event, got %d", len(ordersResult))
	}

	// Verify event content
	for i, expected := range []struct {
		op    cdc.EventType
		table string
	}{{"create", "users"}, {"update", "users"}} {
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
		if decoded.Op != expected.op || decoded.Table != expected.table {
			t.Errorf("users[%d]: got %s/%s, want %s/%s", i, decoded.Op, decoded.Table, expected.op, expected.table)
		}
	}

	// Verify orders event
	dataField, ok := ordersResult[0].Values["data"].(string)
	if !ok {
		t.Fatal("orders[0]: missing 'data' field")
	}
	var decoded cdc.Event
	if err := json.Unmarshal([]byte(dataField), &decoded); err != nil {
		t.Fatalf("orders[0]: unmarshal error = %v", err)
	}
	if decoded.Op != "delete" || decoded.Table != "orders" {
		t.Errorf("orders[0]: got %s/%s, want delete/orders", decoded.Op, decoded.Table)
	}

	client.Del(ctx, usersStream, ordersStream)
}

func TestStreamConfig_Validation(t *testing.T) {
	// Note: Validation happens at runtime in NewRedisStreamSink via Ping
	// This test verifies config fields are properly passed through
	tests := []struct {
		name string
		cfg  StreamConfig
	}{
		{
			name: "valid config",
			cfg: StreamConfig{
				Addr: "localhost:6379",
				TableStreamMap: map[string]string{
					"users": "valid:stream",
				},
			},
		},
		{
			name: "with password",
			cfg: StreamConfig{
				Addr:     "localhost:6379",
				Password: "secret",
				TableStreamMap: map[string]string{
					"users": "stream",
				},
			},
		},
		{
			name: "with DB",
			cfg: StreamConfig{
				Addr: "localhost:6379",
				DB:   5,
				TableStreamMap: map[string]string{
					"users": "stream",
				},
			},
		},
		{
			name: "with max len",
			cfg: StreamConfig{
				Addr:   "localhost:6379",
				MaxLen: 1000,
				TableStreamMap: map[string]string{
					"users": "stream",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if testing.Short() {
				// Skip actual connection test
				return
			}
			sink, err := NewRedisStreamSink(tt.cfg)
			if err != nil {
				// If Redis is not available, skip the test
				t.Skipf("Redis not available: %v", err)
				return
			}
			sink.Close()
		})
	}
}

// Benchmark tests
func BenchmarkRedisStreamSink_Write(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	ctx := context.Background()
	cfg := StreamConfig{
		Addr: "localhost:6379",
		TableStreamMap: map[string]string{
			"users": "bench:cdc:stream",
		},
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		b.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()
	defer func() {
		client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
		client.Del(ctx, "bench:cdc:stream")
	}()

	testEvent := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     "create",
		After:  map[string]any{"id": 1},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sink.Write(ctx, testEvent); err != nil {
			b.Fatal(err)
		}
	}
}
