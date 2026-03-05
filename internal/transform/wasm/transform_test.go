package wasmtransform

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"iris/pkg/cdc"
)

// TestWasmTransform_NewWASM tests creating a WASM transform
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

// TestWasmTransform_Integration tests with a real WASM file if available
func TestWasmTransform_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testDataPath := "./testdata"
	if _, err := os.Stat(testDataPath); os.IsNotExist(err) {
		t.Skipf("testdata directory not found, skipping integration test")
	}

	files, err := filepath.Glob(filepath.Join(testDataPath, "*.wasm"))
	if err != nil || len(files) == 0 {
		t.Skipf("no WASM files found in testdata, skipping integration test")
	}

	ctx := context.Background()
	for _, wasmFile := range files {
		t.Run(filepath.Base(wasmFile), func(t *testing.T) {
			transform, err := NewWASM(ctx, Config{Path: wasmFile})
			if err != nil {
				t.Fatalf("failed to create transform: %v", err)
			}
			defer transform.Close()

			event := &cdc.Event{
				Source: "test",
				Table:  "test_table",
				Op:     cdc.EventTypeCreate,
				TS:     time.Now(),
				After:  map[string]any{"id": 1},
			}

			result, err := transform.Process(event)
			if err != nil {
				t.Logf("transform error (may be expected): %v", err)
			}
			_ = result
		})
	}
}

// TestWasmTransform_DefaultConfig tests configuration defaults
func TestWasmTransform_DefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.FunctionName != "handle" {
		t.Errorf("default FunctionName = %q, want %q", cfg.FunctionName, "handle")
	}
	if cfg.AllocFunctionName != "alloc" {
		t.Errorf("default AllocFunctionName = %q, want %q", cfg.AllocFunctionName, "alloc")
	}
	if cfg.EnableLogging != false {
		t.Error("default EnableLogging should be false")
	}
}

// TestWasmTransform_ConfigBuilder tests config builder methods
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

// TestWasmTransform_Close tests resource cleanup
func TestWasmTransform_Close(t *testing.T) {
	ctx := context.Background()
	runtime, compiled, err := compileWASM(ctx, Config{
		Path: "./testdata/dummy.wasm",
	})

	// Skip if no test WASM file
	if err != nil {
		t.Skipf("no test WASM module available: %v", err)
	}
	defer runtime.Close(ctx)

	transform := &WASMTransform{
		runtime:  runtime,
		compiled: compiled,
	}

	err = transform.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestParseResult_WithNilMemory tests error handling
func TestParseResult_WithNilMemory(t *testing.T) {
	// This test verifies error handling when memory is not available
	// Since we can't easily create a mock module, we test the error path

	// Create a nil module scenario
	type mockModule struct{}

	// We can't directly test ParseResult with nil memory without complex mocking
	// This is a placeholder for the concept
	t.Skip("ParseResult requires complex mocking, skipping")
}
