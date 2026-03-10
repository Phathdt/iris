package wasmtransform

import (
	"context"
	"testing"
	"time"

	"iris/pkg/cdc"
)

func TestWasmTransform_NewWASM(t *testing.T) {
	ctx := context.Background()

	// Test with empty path (should error)
	_, err := NewWASM(ctx, Config{Path: ""})
	if err == nil {
		t.Fatal("expected error for empty WASM path")
	}

	// Test with non-existent file
	_, err = NewWASM(ctx, Config{Path: "/nonexistent/path.wasm"})
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

func TestWasmTransform_Passthrough(t *testing.T) {
	ctx := context.Background()
	transform, err := NewWASM(ctx, Config{Path: "./testdata/passthrough.wasm"})
	if err != nil {
		t.Fatalf("failed to create passthrough transform: %v", err)
	}
	defer transform.Close()

	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		After:  map[string]any{"id": float64(1), "name": "alice"},
	}

	result, err := transform.Process(event)
	if err != nil {
		t.Fatalf("passthrough Process() error: %v", err)
	}
	if result == nil {
		t.Fatal("passthrough Process() returned nil (dropped event)")
	}

	if result.Source != event.Source {
		t.Errorf("Source = %q, want %q", result.Source, event.Source)
	}
	if result.Table != event.Table {
		t.Errorf("Table = %q, want %q", result.Table, event.Table)
	}
	if result.Op != event.Op {
		t.Errorf("Op = %q, want %q", result.Op, event.Op)
	}
	if result.After["name"] != "alice" {
		t.Errorf("After[name] = %v, want %q", result.After["name"], "alice")
	}
}

func TestWasmTransform_FilterAllowed(t *testing.T) {
	ctx := context.Background()
	transform, err := NewWASM(ctx, Config{Path: "./testdata/filter.wasm"})
	if err != nil {
		t.Fatalf("failed to create filter transform: %v", err)
	}
	defer transform.Close()

	// "users" is in the allow list — should pass through
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		After:  map[string]any{"id": float64(1), "name": "alice"},
	}

	result, err := transform.Process(event)
	if err != nil {
		t.Fatalf("filter Process() error for allowed table: %v", err)
	}
	if result == nil {
		t.Fatal("filter dropped allowed table 'users'")
	}
	if result.Table != "users" {
		t.Errorf("Table = %q, want %q", result.Table, "users")
	}
}

func TestWasmTransform_FilterDropped(t *testing.T) {
	ctx := context.Background()
	transform, err := NewWASM(ctx, Config{Path: "./testdata/filter.wasm"})
	if err != nil {
		t.Fatalf("failed to create filter transform: %v", err)
	}
	defer transform.Close()

	// "audit_logs" is NOT in the allow list — should be dropped
	event := &cdc.Event{
		Source: "postgres",
		Table:  "audit_logs",
		Op:     cdc.EventTypeCreate,
		TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		After:  map[string]any{"id": float64(99)},
	}

	result, err := transform.Process(event)
	if err != nil {
		t.Fatalf("filter Process() error for dropped table: %v", err)
	}
	if result != nil {
		t.Errorf("filter should have dropped 'audit_logs', got event: %+v", result)
	}
}

func TestWasmTransform_FilterMultipleTables(t *testing.T) {
	ctx := context.Background()
	transform, err := NewWASM(ctx, Config{Path: "./testdata/filter.wasm"})
	if err != nil {
		t.Fatalf("failed to create filter transform: %v", err)
	}
	defer transform.Close()

	tests := []struct {
		table   string
		allowed bool
	}{
		{"users", true},
		{"orders", true},
		{"audit_logs", false},
		{"sessions", false},
		{"payments", false},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			event := &cdc.Event{
				Source: "postgres",
				Table:  tt.table,
				Op:     cdc.EventTypeCreate,
				TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				After:  map[string]any{"id": float64(1)},
			}

			result, err := transform.Process(event)
			if err != nil {
				t.Fatalf("Process() error: %v", err)
			}

			if tt.allowed && result == nil {
				t.Errorf("expected table %q to pass through, but was dropped", tt.table)
			}
			if !tt.allowed && result != nil {
				t.Errorf("expected table %q to be dropped, but passed through", tt.table)
			}
		})
	}
}

func TestWasmTransform_PassthroughPreservesAllFields(t *testing.T) {
	ctx := context.Background()
	transform, err := NewWASM(ctx, Config{Path: "./testdata/passthrough.wasm"})
	if err != nil {
		t.Fatalf("failed to create transform: %v", err)
	}
	defer transform.Close()

	event := &cdc.Event{
		Source: "postgres",
		Table:  "orders",
		Op:     cdc.EventTypeUpdate,
		TS:     time.Date(2025, 6, 15, 12, 30, 0, 0, time.UTC),
		Before: map[string]any{"id": float64(42), "status": "pending"},
		After:  map[string]any{"id": float64(42), "status": "shipped"},
	}

	result, err := transform.Process(event)
	if err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if result == nil {
		t.Fatal("Process() returned nil")
	}

	if result.Op != cdc.EventTypeUpdate {
		t.Errorf("Op = %q, want %q", result.Op, cdc.EventTypeUpdate)
	}
	if result.Before["status"] != "pending" {
		t.Errorf("Before[status] = %v, want %q", result.Before["status"], "pending")
	}
	if result.After["status"] != "shipped" {
		t.Errorf("After[status] = %v, want %q", result.After["status"], "shipped")
	}
}

func TestWasmTransform_Close(t *testing.T) {
	ctx := context.Background()
	transform, err := NewWASM(ctx, Config{Path: "./testdata/passthrough.wasm"})
	if err != nil {
		t.Fatalf("failed to create transform: %v", err)
	}

	if err := transform.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestWasmTransform_TinyGo_Passthrough tests TinyGo-compiled passthrough module
func TestWasmTransform_TinyGo_Passthrough(t *testing.T) {
	ctx := context.Background()
	transform, err := NewWASM(ctx, Config{Path: "./testdata/passthrough-tinygo.wasm"})
	if err != nil {
		t.Fatalf("failed to create TinyGo passthrough transform: %v", err)
	}
	defer transform.Close()

	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		After:  map[string]any{"id": float64(1), "name": "alice"},
	}

	result, err := transform.Process(event)
	if err != nil {
		t.Fatalf("TinyGo passthrough Process() error: %v", err)
	}
	if result == nil {
		t.Fatal("TinyGo passthrough returned nil")
	}
	if result.Source != "postgres" {
		t.Errorf("Source = %q, want %q", result.Source, "postgres")
	}
	if result.After["name"] != "alice" {
		t.Errorf("After[name] = %v, want %q", result.After["name"], "alice")
	}
}

// TestWasmTransform_TinyGo_Filter tests TinyGo-compiled filter module
func TestWasmTransform_TinyGo_Filter(t *testing.T) {
	ctx := context.Background()
	transform, err := NewWASM(ctx, Config{Path: "./testdata/filter-tinygo.wasm"})
	if err != nil {
		t.Fatalf("failed to create TinyGo filter transform: %v", err)
	}
	defer transform.Close()

	tests := []struct {
		table   string
		allowed bool
	}{
		{"users", true},
		{"orders", true},
		{"audit_logs", false},
		{"sessions", false},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			event := &cdc.Event{
				Source: "postgres",
				Table:  tt.table,
				Op:     cdc.EventTypeCreate,
				TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				After:  map[string]any{"id": float64(1)},
			}

			result, err := transform.Process(event)
			if err != nil {
				t.Fatalf("Process() error: %v", err)
			}

			if tt.allowed && result == nil {
				t.Errorf("expected table %q to pass, but was dropped", tt.table)
			}
			if !tt.allowed && result != nil {
				t.Errorf("expected table %q to be dropped, but passed", tt.table)
			}
		})
	}
}

func TestWasmTransform_DefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.FunctionName != "handle" {
		t.Errorf("default FunctionName = %q, want %q", cfg.FunctionName, "handle")
	}
	if cfg.AllocFunctionName != "alloc" {
		t.Errorf("default AllocFunctionName = %q, want %q", cfg.AllocFunctionName, "alloc")
	}
}

func TestWasmTransform_ConfigBuilder(t *testing.T) {
	cfg := DefaultConfig().
		WithPath("/path/to/module.wasm").
		WithFunctionName("transform").
		WithAllocFunctionName("allocate").
		WithLogging(true)

	if cfg.Path != "/path/to/module.wasm" {
		t.Errorf("WithPath failed: got %q", cfg.Path)
	}
	if cfg.FunctionName != "transform" {
		t.Errorf("WithFunctionName failed: got %q", cfg.FunctionName)
	}
	if cfg.AllocFunctionName != "allocate" {
		t.Errorf("WithAllocFunctionName failed: got %q", cfg.AllocFunctionName)
	}
	if cfg.EnableLogging != true {
		t.Error("WithLogging failed")
	}
}
