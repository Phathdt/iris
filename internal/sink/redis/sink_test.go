package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{
		Addr: "localhost:6379",
		Key:  "test:key",
	}

	if cfg.DB != 0 {
		t.Errorf("default DB should be 0, got %d", cfg.DB)
	}
	if cfg.MaxLen != 0 {
		t.Errorf("default MaxLen should be 0, got %d", cfg.MaxLen)
	}
}

func TestRedisSink_NewRedisSink_InvalidAddr(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Test with invalid address
	cfg := Config{
		Addr: "invalid-address:99999",
		Key:  "test:key",
	}

	_, err := NewRedisSink(cfg)
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisSink_NewRedisSink_ConnectionError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Test with unreachable address
	cfg := Config{
		Addr: "localhost:1", // Port 1 is typically closed
		Key:  "test:key",
	}

	_, err := NewRedisSink(cfg)
	if err == nil {
		t.Fatal("expected error for unreachable Redis")
	}
}

func TestRedisSink_Write(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// This test requires a running Redis instance
	ctx := context.Background()

	cfg := Config{
		Addr: "localhost:6379",
		Key:  "test:cdc:events:unit",
	}

	sink, err := NewRedisSink(cfg)
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

	result, err := client.LRange(ctx, cfg.Key, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange error = %v", err)
	}

	if len(result) == 0 {
		t.Fatal("expected data in Redis list")
	}

	if string(result[0]) != string(testData) {
		t.Errorf("data mismatch: got %q, want %q", result[0], testData)
	}

	// Cleanup
	client.Del(ctx, cfg.Key)
}

func TestRedisSink_Write_MaxLen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	cfg := Config{
		Addr:   "localhost:6379",
		Key:    "test:cdc:events:maxlen",
		MaxLen: 3,
	}

	sink, err := NewRedisSink(cfg)
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

	// Verify list is trimmed to MaxLen
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
	defer client.Close()

	result, err := client.LLen(ctx, cfg.Key).Result()
	if err != nil {
		t.Fatalf("LLen error = %v", err)
	}

	if result != int64(cfg.MaxLen) {
		t.Errorf("list length = %d, want %d", result, cfg.MaxLen)
	}

	client.Del(ctx, cfg.Key)
}

func TestRedisSink_Write_ContextCancelled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	cfg := Config{
		Addr: "localhost:6379",
		Key:  "test:key",
	}

	sink, err := NewRedisSink(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()

	err = sink.Write(ctx, []byte("test"))
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestRedisSink_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cfg := Config{
		Addr: "localhost:6379",
		Key:  "test:key",
	}

	sink, err := NewRedisSink(cfg)
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

func TestRedisSink_Integration_FullFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	cfg := Config{
		Addr: "localhost:6379",
		Key:  "test:cdc:fullflow",
	}

	sink, err := NewRedisSink(cfg)
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

	result, err := client.LRange(ctx, cfg.Key, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange error = %v", err)
	}

	if len(result) != len(events) {
		t.Fatalf("expected %d events, got %d", len(events), len(result))
	}

	// Note: LPUSH adds to front, so order is reversed
	for i, expected := range events {
		// Reverse index because LPUSH
		idx := len(events) - 1 - i
		if idx < 0 || idx >= len(result) {
			continue
		}
		if string(result[idx]) != string(expected) {
			t.Errorf("event[%d]: got %q, want %q", i, result[idx], expected)
		}
	}

	client.Del(ctx, cfg.Key)
}

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				Addr: "localhost:6379",
				Key:  "valid:key",
			},
			wantErr: false,
		},
		{
			name: "empty addr",
			cfg: Config{
				Addr: "",
				Key:  "key",
			},
			wantErr: true,
		},
		{
			name: "empty key",
			cfg: Config{
				Addr: "localhost:6379",
				Key:  "",
			},
			wantErr: true,
		},
		{
			name: "with password",
			cfg: Config{
				Addr:     "localhost:6379",
				Password: "secret",
				Key:      "key",
			},
			wantErr: false,
		},
		{
			name: "with DB",
			cfg: Config{
				Addr: "localhost:6379",
				DB:   5,
				Key:  "key",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Note: validation happens in NewRedisSink via Ping
			_, err := NewRedisSink(tt.cfg)
			if testing.Short() {
				// Skip actual connection test
				return
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRedisSink() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Benchmark tests
func BenchmarkRedisSink_Write(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	ctx := context.Background()
	cfg := Config{
		Addr: "localhost:6379",
		Key:  "bench:cdc:events",
	}

	sink, err := NewRedisSink(cfg)
	if err != nil {
		b.Skipf("Redis not available: %v", err)
	}
	defer sink.Close()
	defer func() {
		client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
		client.Del(ctx, cfg.Key)
	}()

	testData := []byte(`{"source":"postgres","table":"users","op":"create","after":{"id":1}}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sink.Write(ctx, testData); err != nil {
			b.Fatal(err)
		}
	}
}
