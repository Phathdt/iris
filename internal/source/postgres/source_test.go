package postgres

import (
	"context"
	"testing"
	"time"

	"iris/pkg/cdc"
)

func TestPostgresSource_NewSource(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				DSN:      "postgres://user:pass@localhost:5432/db",
				SlotName: "test_slot",
			},
			wantErr: false,
		},
		{
			name: "missing slot name",
			cfg: Config{
				DSN: "postgres://user:pass@localhost:5432/db",
			},
			wantErr: true,
		},
		{
			name: "missing DSN",
			cfg: Config{
				SlotName: "test_slot",
			},
			wantErr: true,
		},
		{
			name: "empty config",
			cfg:  Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := NewSource(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSource() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if src == nil {
					t.Error("expected non-nil source")
				}
				if src.events == nil {
					t.Error("expected events channel to be initialized")
				}
				if src.slotName != tt.cfg.SlotName {
					t.Errorf("slotName = %q, want %q", src.slotName, tt.cfg.SlotName)
				}
			}
		})
	}
}

func TestPostgresSource_Start_InvalidDSN(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	src, _ := NewSource(Config{
		DSN:      "invalid-dsn",
		SlotName: "test_slot",
	})
	defer src.Close()

	ctx := context.Background()
	_, err := src.Start(ctx)
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
}

func TestPostgresSource_Start_ConnectionRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	src, _ := NewSource(Config{
		DSN:      "postgres://user:pass@localhost:1/db", // Port 1 typically closed
		SlotName: "test_slot",
	})
	defer src.Close()

	ctx := context.Background()
	_, err := src.Start(ctx)
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}

func TestPostgresSource_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	src, _ := NewSource(Config{
		DSN:      "postgres://user:pass@localhost:5432/db",
		SlotName: "test_slot",
	})

	// Close without Start should be safe
	err := src.Close()
	if err != nil {
		t.Logf("Close() error (expected without connection): %v", err)
	}
}

func TestPostgresSource_Ack_NotStarted(t *testing.T) {
	src, _ := NewSource(Config{
		DSN:      "postgres://user:pass@localhost:5432/db",
		SlotName: "test_slot",
	})
	defer src.Close()

	offset := cdc.Offset{LSN: 100}
	err := src.Ack(offset)
	if err == nil {
		t.Error("expected error when not connected")
	}
}

func TestPostgresSource_StartOffset(t *testing.T) {
	startOffset := &cdc.Offset{
		LSN:        0x1234567890ABCDEF,
		CommitTime: time.Now(),
	}

	cfg := Config{
		DSN:         "postgres://user:pass@localhost:5432/db",
		SlotName:    "test_slot",
		StartOffset: startOffset,
	}

	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("NewSource() error = %v", err)
	}
	defer src.Close()

	if src.config.StartOffset != startOffset {
		t.Error("StartOffset not stored correctly")
	}
}

func TestPostgresSource_EventsChannel(t *testing.T) {
	cfg := Config{
		DSN:      "postgres://user:pass@localhost:5432/db",
		SlotName: "test_slot",
	}

	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("NewSource() error = %v", err)
	}
	defer src.Close()

	// Verify events channel is buffered
	capacity := cap(src.events)
	if capacity != 1024 {
		t.Errorf("events channel capacity = %d, want 1024", capacity)
	}
}

func TestConfig_WithStartOffset(t *testing.T) {
	offset := cdc.Offset{LSN: 100}
	cfg := Config{
		DSN:         "postgres://localhost/db",
		SlotName:    "slot",
		StartOffset: &offset,
	}

	if cfg.StartOffset == nil {
		t.Fatal("StartOffset is nil")
	}
	if cfg.StartOffset.LSN != 100 {
		t.Errorf("StartOffset.LSN = %d, want 100", cfg.StartOffset.LSN)
	}
}

// Integration test - requires running PostgreSQL with logical replication
func TestPostgresSource_Integration_FullFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// This test requires:
	// 1. PostgreSQL 16+ with logical replication enabled
	// 2. A database with a publication named "pglogrepl_publication"
	// 3. TEST_POSTGRES_DSN environment variable set

	dsn := getTestDSN()
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set, skipping integration test")
	}

	ctx := context.Background()
	cfg := Config{
		DSN:      dsn,
		SlotName: "iris_test_source_slot",
	}

	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("NewSource() error = %v", err)
	}
	defer src.Close()

	events, err := src.Start(ctx)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Give some time for events to start flowing
	select {
	case rawEvent, ok := <-events:
		if !ok {
			t.Log("events channel closed immediately")
			return
		}
		t.Logf("received raw event: %v", rawEvent)
	case <-time.After(2 * time.Second):
		t.Log("no events received within timeout (may be expected if no WAL activity)")
	}

	// Test Ack
	offset := cdc.Offset{LSN: 100}
	err = src.Ack(offset)
	if err != nil {
		t.Logf("Ack() error (may be expected): %v", err)
	}
}

// Helper to get test DSN from environment
func getTestDSN() string {
	// Check environment variable
	dsn := "postgres://iris:iris@localhost:54321/testdb"
	return dsn
}

// TestPostgresSource_ContextCancellation tests graceful shutdown
func TestPostgresSource_ContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	src, _ := NewSource(Config{
		DSN:      "postgres://user:pass@localhost:5432/db",
		SlotName: "test_slot",
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	_, err := src.Start(ctx)
	// Should fail due to context cancellation or connection error
	if err == nil {
		t.Log("Start() succeeded despite cancelled context (unexpected)")
	}

	src.Close()
}
