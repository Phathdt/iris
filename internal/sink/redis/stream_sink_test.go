package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestStreamConfig_Defaults(t *testing.T) {
	cfg := StreamConfig{
		Addr:      "localhost:6379",
		StreamKey: "test:stream",
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
		Addr:      "invalid-address:99999",
		StreamKey: "test:stream",
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
		Addr:      "localhost:1", // Port 1 is typically closed
		StreamKey: "test:stream",
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
		Addr:      "localhost:6379",
		StreamKey: "test:cdc:stream:unit",
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	testData := []byte(`{"source":"postgres","table":"users","op":"create"}`)

	err = sink.Write(ctx, testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify data was written
	client := redis.NewClient(&redis.Options{
		Addr: cfg.Addr,
	})
	defer client.Close()

	result, err := client.XRange(ctx, cfg.StreamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	if len(result) == 0 {
		t.Fatal("expected data in Redis stream")
	}

	// Check the 'data' field
	entry := result[0]
	dataField, ok := entry.Values["data"]
	if !ok {
		t.Fatal("expected 'data' field in stream entry")
	}
	if dataField != string(testData) {
		t.Errorf("data mismatch: got %q, want %q", dataField, testData)
	}

	// Cleanup
	client.Del(ctx, cfg.StreamKey)
}

func TestRedisStreamSink_Write_MaxLen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	cfg := StreamConfig{
		Addr:      "localhost:6379",
		StreamKey: "test:cdc:stream:maxlen",
		MaxLen:    3,
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	// Write more items than MaxLen
	for i := 0; i < 5; i++ {
		data := []byte(`{"id":` + string(rune('0'+i)) + `}`)
		if err := sink.Write(ctx, data); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Verify stream is trimmed to MaxLen
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
	defer client.Close()

	result, err := client.XRange(ctx, cfg.StreamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	if len(result) != cfg.MaxLen {
		t.Errorf("stream length = %d, want %d", len(result), cfg.MaxLen)
	}

	client.Del(ctx, cfg.StreamKey)
}

func TestRedisStreamSink_Write_MaxLenApprox(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	cfg := StreamConfig{
		Addr:            "localhost:6379",
		StreamKey:       "test:cdc:stream:maxlenapprox",
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
		data := []byte(`{"id":` + string(rune('0'+i)) + `}`)
		if err := sink.Write(ctx, data); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Verify stream is trimmed (approximately) to MaxLen
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
	defer client.Close()

	result, err := client.XRange(ctx, cfg.StreamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	// With approximate trimming, length should be around MaxLen
	if len(result) < cfg.MaxLen-1 || len(result) > cfg.MaxLen+2 {
		t.Errorf("stream length = %d, expected around %d", len(result), cfg.MaxLen)
	}

	client.Del(ctx, cfg.StreamKey)
}

func TestRedisStreamSink_Write_ContextCancelled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	cfg := StreamConfig{
		Addr:      "localhost:6379",
		StreamKey: "test:stream",
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	err = sink.Write(ctx, []byte("test"))
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestRedisStreamSink_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cfg := StreamConfig{
		Addr:      "localhost:6379",
		StreamKey: "test:stream",
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
	cfg := StreamConfig{
		Addr:      "localhost:6379",
		StreamKey: "test:cdc:fullflow:stream",
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	// Write multiple events
	events := [][]byte{
		[]byte(`{"op":"create","table":"users"}`),
		[]byte(`{"op":"update","table":"users"}`),
		[]byte(`{"op":"delete","table":"orders"}`),
	}

	for _, event := range events {
		if err := sink.Write(ctx, event); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Read and verify
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
	defer client.Close()

	result, err := client.XRange(ctx, cfg.StreamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error = %v", err)
	}

	if len(result) != len(events) {
		t.Fatalf("expected %d events, got %d", len(events), len(result))
	}

	// Verify order (streams maintain chronological order)
	for i, expected := range events {
		if i >= len(result) {
			break
		}
		dataField, ok := result[i].Values["data"]
		if !ok {
			t.Errorf("event[%d]: missing 'data' field", i)
			continue
		}
		if dataField != string(expected) {
			t.Errorf("event[%d]: got %q, want %q", i, dataField, expected)
		}
	}

	client.Del(ctx, cfg.StreamKey)
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
				Addr:      "localhost:6379",
				StreamKey: "valid:stream",
			},
		},
		{
			name: "with password",
			cfg: StreamConfig{
				Addr:      "localhost:6379",
				Password:  "secret",
				StreamKey: "stream",
			},
		},
		{
			name: "with DB",
			cfg: StreamConfig{
				Addr:      "localhost:6379",
				DB:        5,
				StreamKey: "stream",
			},
		},
		{
			name: "with max len",
			cfg: StreamConfig{
				Addr:      "localhost:6379",
				MaxLen:    1000,
				StreamKey: "stream",
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
		Addr:      "localhost:6379",
		StreamKey: "bench:cdc:stream",
	}

	sink, err := NewRedisStreamSink(cfg)
	if err != nil {
		b.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()
	defer func() {
		client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
		client.Del(ctx, cfg.StreamKey)
	}()

	testData := []byte(`{"source":"postgres","table":"users","op":"create","after":{"id":1}}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sink.Write(ctx, testData); err != nil {
			b.Fatal(err)
		}
	}
}
