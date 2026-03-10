package wasmtransform

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"iris/pkg/cdc"
)

// wasmVariant represents a WASM binary variant for testing
type wasmVariant struct {
	name            string
	passthroughPath string
	filterPath      string
}

var variants = []wasmVariant{
	{"Rust", "./testdata/passthrough.wasm", "./testdata/filter.wasm"},
	{"TinyGo", "./testdata/passthrough-tinygo.wasm", "./testdata/filter-tinygo.wasm"},
}

// --- Unit Tests: Error Paths ---

func TestNewWASM_EmptyPath(t *testing.T) {
	_, err := NewWASM(context.Background(), Config{Path: ""})
	if err == nil {
		t.Fatal("expected error for empty WASM path")
	}
}

func TestNewWASM_NonExistentFile(t *testing.T) {
	_, err := NewWASM(context.Background(), Config{Path: "/nonexistent/path.wasm"})
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

func TestNewWASM_InvalidWASMBytes(t *testing.T) {
	// Write a temporary file with invalid WASM content
	tmpFile := t.TempDir() + "/invalid.wasm"
	if err := writeFile(tmpFile, []byte("not a wasm module")); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	_, err := NewWASM(context.Background(), Config{Path: tmpFile})
	if err == nil {
		t.Fatal("expected error for invalid WASM bytes")
	}
}

// --- Unit Tests: Config ---

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.FunctionName != "handle" {
		t.Errorf("FunctionName = %q, want %q", cfg.FunctionName, "handle")
	}
	if cfg.AllocFunctionName != "alloc" {
		t.Errorf("AllocFunctionName = %q, want %q", cfg.AllocFunctionName, "alloc")
	}
	if cfg.EnableLogging {
		t.Error("EnableLogging should default to false")
	}
}

func TestConfigBuilder(t *testing.T) {
	cfg := DefaultConfig().
		WithPath("/path/to/module.wasm").
		WithFunctionName("transform").
		WithAllocFunctionName("allocate").
		WithLogging(true)

	if cfg.Path != "/path/to/module.wasm" {
		t.Errorf("Path = %q, want %q", cfg.Path, "/path/to/module.wasm")
	}
	if cfg.FunctionName != "transform" {
		t.Errorf("FunctionName = %q, want %q", cfg.FunctionName, "transform")
	}
	if cfg.AllocFunctionName != "allocate" {
		t.Errorf("AllocFunctionName = %q, want %q", cfg.AllocFunctionName, "allocate")
	}
	if !cfg.EnableLogging {
		t.Error("EnableLogging should be true")
	}
}

func TestNewWASM_DefaultFunctionNames(t *testing.T) {
	ctx := context.Background()
	// Use passthrough.wasm (Rust) which has alloc + handle exports
	transform, err := NewWASM(ctx, Config{Path: "./testdata/passthrough.wasm"})
	if err != nil {
		t.Fatalf("NewWASM error: %v", err)
	}
	defer transform.Close()

	if transform.allocFnName != "alloc" {
		t.Errorf("allocFnName = %q, want %q", transform.allocFnName, "alloc")
	}
	if transform.handleFnName != "handle" {
		t.Errorf("handleFnName = %q, want %q", transform.handleFnName, "handle")
	}
}

// --- Unit Tests: Passthrough (both variants) ---

func TestPassthrough_BasicEvent(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.passthroughPath)
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
				t.Fatalf("Process() error: %v", err)
			}
			if result == nil {
				t.Fatal("Process() returned nil")
			}

			assertEventEqual(t, event, result)
		})
	}
}

func TestPassthrough_UpdateWithBeforeAfter(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.passthroughPath)
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

			assertEventEqual(t, event, result)
			if result.Before["status"] != "pending" {
				t.Errorf("Before[status] = %v, want %q", result.Before["status"], "pending")
			}
			if result.After["status"] != "shipped" {
				t.Errorf("After[status] = %v, want %q", result.After["status"], "shipped")
			}
		})
	}
}

func TestPassthrough_DeleteEvent(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.passthroughPath)
			defer transform.Close()

			event := &cdc.Event{
				Source: "postgres",
				Table:  "users",
				Op:     cdc.EventTypeDelete,
				TS:     time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
				Before: map[string]any{"id": float64(99), "name": "bob"},
			}

			result, err := transform.Process(event)
			if err != nil {
				t.Fatalf("Process() error: %v", err)
			}
			if result == nil {
				t.Fatal("Process() returned nil")
			}

			if result.Op != cdc.EventTypeDelete {
				t.Errorf("Op = %q, want %q", result.Op, cdc.EventTypeDelete)
			}
			if result.Before["name"] != "bob" {
				t.Errorf("Before[name] = %v, want %q", result.Before["name"], "bob")
			}
			if result.After != nil {
				t.Errorf("After should be nil for delete, got %v", result.After)
			}
		})
	}
}

func TestPassthrough_EmptyAfter(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.passthroughPath)
			defer transform.Close()

			event := &cdc.Event{
				Source: "postgres",
				Table:  "users",
				Op:     cdc.EventTypeCreate,
				TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				After:  map[string]any{},
			}

			result, err := transform.Process(event)
			if err != nil {
				t.Fatalf("Process() error: %v", err)
			}
			if result == nil {
				t.Fatal("Process() returned nil")
			}
			if result.Table != "users" {
				t.Errorf("Table = %q, want %q", result.Table, "users")
			}
		})
	}
}

func TestPassthrough_LargePayload(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.passthroughPath)
			defer transform.Close()

			// Build a large After map (~50 fields)
			after := make(map[string]any)
			for i := range 50 {
				after[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value_%d_with_some_padding_data", i)
			}

			event := &cdc.Event{
				Source: "postgres",
				Table:  "big_table",
				Op:     cdc.EventTypeCreate,
				TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				After:  after,
			}

			result, err := transform.Process(event)
			if err != nil {
				t.Fatalf("Process() error: %v", err)
			}
			if result == nil {
				t.Fatal("Process() returned nil")
			}
			if len(result.After) != 50 {
				t.Errorf("After has %d fields, want 50", len(result.After))
			}
		})
	}
}

func TestPassthrough_SpecialCharacters(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.passthroughPath)
			defer transform.Close()

			event := &cdc.Event{
				Source: "postgres",
				Table:  "users",
				Op:     cdc.EventTypeCreate,
				TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				After: map[string]any{
					"name":    "alice \"bob\" <carol>",
					"bio":     "line1\nline2\ttab",
					"emoji":   "\u2603",
					"unicode": "\u4e16\u754c",
				},
			}

			result, err := transform.Process(event)
			if err != nil {
				t.Fatalf("Process() error: %v", err)
			}
			if result == nil {
				t.Fatal("Process() returned nil")
			}
			if result.After["name"] != "alice \"bob\" <carol>" {
				t.Errorf("name = %v, want %q", result.After["name"], "alice \"bob\" <carol>")
			}
			if result.After["emoji"] != "\u2603" {
				t.Errorf("emoji = %v, want snowman", result.After["emoji"])
			}
		})
	}
}

func TestPassthrough_NumericTypes(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.passthroughPath)
			defer transform.Close()

			event := &cdc.Event{
				Source: "postgres",
				Table:  "metrics",
				Op:     cdc.EventTypeCreate,
				TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				After: map[string]any{
					"int_val":   float64(42),
					"float_val": float64(3.14),
					"zero":      float64(0),
					"negative":  float64(-100),
					"bool_val":  true,
					"null_val":  nil,
				},
			}

			result, err := transform.Process(event)
			if err != nil {
				t.Fatalf("Process() error: %v", err)
			}
			if result == nil {
				t.Fatal("Process() returned nil")
			}
			if result.After["float_val"] != float64(3.14) {
				t.Errorf("float_val = %v, want 3.14", result.After["float_val"])
			}
			if result.After["bool_val"] != true {
				t.Errorf("bool_val = %v, want true", result.After["bool_val"])
			}
			if result.After["null_val"] != nil {
				t.Errorf("null_val = %v, want nil", result.After["null_val"])
			}
		})
	}
}

func TestPassthrough_MultipleCallsOnSameTransform(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.passthroughPath)
			defer transform.Close()

			for i := range 10 {
				event := &cdc.Event{
					Source: "postgres",
					Table:  "users",
					Op:     cdc.EventTypeCreate,
					TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					After:  map[string]any{"id": float64(i)},
				}

				result, err := transform.Process(event)
				if err != nil {
					t.Fatalf("Process() call %d error: %v", i, err)
				}
				if result == nil {
					t.Fatalf("Process() call %d returned nil", i)
				}
				if result.After["id"] != float64(i) {
					t.Errorf("call %d: After[id] = %v, want %v", i, result.After["id"], float64(i))
				}
			}
		})
	}
}

// --- Unit Tests: Filter (both variants) ---

func TestFilter_AllowedTables(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.filterPath)
			defer transform.Close()

			for _, table := range []string{"users", "orders"} {
				t.Run(table, func(t *testing.T) {
					event := &cdc.Event{
						Source: "postgres",
						Table:  table,
						Op:     cdc.EventTypeCreate,
						TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
						After:  map[string]any{"id": float64(1), "name": "test"},
					}

					result, err := transform.Process(event)
					if err != nil {
						t.Fatalf("Process() error: %v", err)
					}
					if result == nil {
						t.Fatalf("expected table %q to pass, but was dropped", table)
					}
					if result.Table != table {
						t.Errorf("Table = %q, want %q", result.Table, table)
					}
					if result.After["name"] != "test" {
						t.Errorf("After[name] = %v, want %q", result.After["name"], "test")
					}
				})
			}
		})
	}
}

func TestFilter_DroppedTables(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.filterPath)
			defer transform.Close()

			droppedTables := []string{"audit_logs", "sessions", "payments", "analytics", "temp_data"}
			for _, table := range droppedTables {
				t.Run(table, func(t *testing.T) {
					event := &cdc.Event{
						Source: "postgres",
						Table:  table,
						Op:     cdc.EventTypeCreate,
						TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
						After:  map[string]any{"id": float64(1)},
					}

					result, err := transform.Process(event)
					if err != nil {
						t.Fatalf("Process() error: %v", err)
					}
					if result != nil {
						t.Errorf("expected table %q to be dropped, but got event", table)
					}
				})
			}
		})
	}
}

func TestFilter_PreservesFieldsOnAllowed(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.filterPath)
			defer transform.Close()

			event := &cdc.Event{
				Source: "postgres",
				Table:  "users",
				Op:     cdc.EventTypeUpdate,
				TS:     time.Date(2025, 6, 15, 12, 30, 0, 0, time.UTC),
				Before: map[string]any{"id": float64(1), "email": "old@test.com"},
				After:  map[string]any{"id": float64(1), "email": "new@test.com"},
			}

			result, err := transform.Process(event)
			if err != nil {
				t.Fatalf("Process() error: %v", err)
			}
			if result == nil {
				t.Fatal("Process() returned nil for allowed table")
			}

			assertEventEqual(t, event, result)
		})
	}
}

func TestFilter_MixedSequence(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.filterPath)
			defer transform.Close()

			sequence := []struct {
				table    string
				expected bool
			}{
				{"users", true},
				{"audit_logs", false},
				{"orders", true},
				{"sessions", false},
				{"users", true},
				{"payments", false},
				{"orders", true},
			}

			for i, s := range sequence {
				event := &cdc.Event{
					Source: "postgres",
					Table:  s.table,
					Op:     cdc.EventTypeCreate,
					TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					After:  map[string]any{"seq": float64(i)},
				}

				result, err := transform.Process(event)
				if err != nil {
					t.Fatalf("seq %d (%s): Process() error: %v", i, s.table, err)
				}

				if s.expected && result == nil {
					t.Errorf("seq %d (%s): expected pass, got drop", i, s.table)
				}
				if !s.expected && result != nil {
					t.Errorf("seq %d (%s): expected drop, got pass", i, s.table)
				}
			}
		})
	}
}

func TestFilter_AllOperationTypes(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			transform := mustCreateTransform(t, v.filterPath)
			defer transform.Close()

			ops := []cdc.EventType{cdc.EventTypeCreate, cdc.EventTypeUpdate, cdc.EventTypeDelete}
			for _, op := range ops {
				t.Run(string(op), func(t *testing.T) {
					event := &cdc.Event{
						Source: "postgres",
						Table:  "users",
						Op:     op,
						TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					}
					if op != cdc.EventTypeDelete {
						event.After = map[string]any{"id": float64(1)}
					}
					if op == cdc.EventTypeUpdate || op == cdc.EventTypeDelete {
						event.Before = map[string]any{"id": float64(1)}
					}

					result, err := transform.Process(event)
					if err != nil {
						t.Fatalf("Process() error: %v", err)
					}
					if result == nil {
						t.Fatal("allowed table should pass for all op types")
					}
					if result.Op != op {
						t.Errorf("Op = %q, want %q", result.Op, op)
					}
				})
			}
		})
	}
}

// --- Unit Tests: Resource Management ---

func TestClose_Idempotent(t *testing.T) {
	transform := mustCreateTransform(t, "./testdata/passthrough.wasm")

	if err := transform.Close(); err != nil {
		t.Errorf("first Close() error: %v", err)
	}
	// Second close should not panic
	_ = transform.Close()
}

func TestProcess_AfterClose(t *testing.T) {
	transform := mustCreateTransform(t, "./testdata/passthrough.wasm")
	transform.Close()

	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Now(),
		After:  map[string]any{"id": float64(1)},
	}

	_, err := transform.Process(event)
	if err == nil {
		t.Error("expected error when calling Process after Close")
	}
}

func TestCDCTransformInterface(t *testing.T) {
	// Verify WASMTransform satisfies cdc.Transform at compile time
	var _ cdc.Transform = (*WASMTransform)(nil)
}

// --- Helpers ---

func mustCreateTransform(t *testing.T, path string) *WASMTransform {
	t.Helper()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Skipf("WASM module not found at %s, skipping test (build with make build-wasm or build-wasm-tinygo)", path)
	}
	transform, err := NewWASM(context.Background(), Config{Path: path})
	if err != nil {
		t.Fatalf("failed to create transform from %s: %v", path, err)
	}
	return transform
}

func assertEventEqual(t *testing.T, want, got *cdc.Event) {
	t.Helper()
	if got.Source != want.Source {
		t.Errorf("Source = %q, want %q", got.Source, want.Source)
	}
	if got.Table != want.Table {
		t.Errorf("Table = %q, want %q", got.Table, want.Table)
	}
	if got.Op != want.Op {
		t.Errorf("Op = %q, want %q", got.Op, want.Op)
	}
}

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}
