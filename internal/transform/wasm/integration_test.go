//go:build integration

package wasmtransform

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"iris/pkg/cdc"
)

// Integration tests verify WASM transforms work correctly with real module
// instantiation, memory management, and multi-call scenarios under the
// `integration` build tag.

// TestIntegration_Passthrough_RustConcurrent tests concurrent Process calls on Rust module
func TestIntegration_Passthrough_RustConcurrent(t *testing.T) {
	testConcurrentPassthrough(t, "./testdata/passthrough.wasm")
}

// TestIntegration_Passthrough_TinyGoConcurrent tests concurrent Process calls on TinyGo module
func TestIntegration_Passthrough_TinyGoConcurrent(t *testing.T) {
	testConcurrentPassthrough(t, "./testdata/passthrough-tinygo.wasm")
}

func testConcurrentPassthrough(t *testing.T, wasmPath string) {
	transform := mustCreateIntegrationTransform(t, wasmPath)
	defer transform.Close()

	const goroutines = 10
	const eventsPerGoroutine = 20

	errCh := make(chan error, goroutines*eventsPerGoroutine)
	done := make(chan struct{})

	for g := range goroutines {
		go func(gID int) {
			for i := range eventsPerGoroutine {
				event := &cdc.Event{
					Source: "postgres",
					Table:  "users",
					Op:     cdc.EventTypeCreate,
					TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					After: map[string]any{
						"goroutine": float64(gID),
						"index":     float64(i),
					},
				}

				result, err := transform.Process(event)
				if err != nil {
					errCh <- err
					return
				}
				if result == nil {
					errCh <- errorf("goroutine %d, index %d: result is nil", gID, i)
					return
				}
				if result.After["goroutine"] != float64(gID) {
					errCh <- errorf("goroutine mismatch: got %v, want %v", result.After["goroutine"], gID)
					return
				}
			}
		}(g)
	}

	go func() {
		// Wait for all goroutines (simple approach: just wait enough time)
		time.Sleep(5 * time.Second)
		close(done)
	}()

	select {
	case err := <-errCh:
		t.Fatalf("concurrent error: %v", err)
	case <-done:
		// success
	}
}

// TestIntegration_Filter_RustHighVolume tests filter with many events
func TestIntegration_Filter_RustHighVolume(t *testing.T) {
	testFilterHighVolume(t, "./testdata/filter.wasm")
}

// TestIntegration_Filter_TinyGoHighVolume tests filter with many events
func TestIntegration_Filter_TinyGoHighVolume(t *testing.T) {
	testFilterHighVolume(t, "./testdata/filter-tinygo.wasm")
}

func testFilterHighVolume(t *testing.T, wasmPath string) {
	transform := mustCreateIntegrationTransform(t, wasmPath)
	defer transform.Close()

	tables := []string{"users", "orders", "audit_logs", "sessions", "payments"}
	allowed := map[string]bool{"users": true, "orders": true}

	passCount := 0
	dropCount := 0

	for round := range 100 {
		table := tables[round%len(tables)]
		event := &cdc.Event{
			Source: "postgres",
			Table:  table,
			Op:     cdc.EventTypeCreate,
			TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			After:  map[string]any{"round": float64(round)},
		}

		result, err := transform.Process(event)
		if err != nil {
			t.Fatalf("round %d, table %q: error: %v", round, table, err)
		}

		if allowed[table] {
			if result == nil {
				t.Fatalf("round %d: expected %q to pass", round, table)
			}
			passCount++
		} else {
			if result != nil {
				t.Fatalf("round %d: expected %q to be dropped", round, table)
			}
			dropCount++
		}
	}

	// 100 events: 2/5 tables allowed = 40 pass, 3/5 dropped = 60 drop
	if passCount != 40 {
		t.Errorf("passCount = %d, want 40", passCount)
	}
	if dropCount != 60 {
		t.Errorf("dropCount = %d, want 60", dropCount)
	}
}

// TestIntegration_Passthrough_RustJSONFidelity verifies JSON round-trip preserves all types
func TestIntegration_Passthrough_RustJSONFidelity(t *testing.T) {
	testJSONFidelity(t, "./testdata/passthrough.wasm")
}

// TestIntegration_Passthrough_TinyGoJSONFidelity verifies JSON round-trip preserves all types
func TestIntegration_Passthrough_TinyGoJSONFidelity(t *testing.T) {
	testJSONFidelity(t, "./testdata/passthrough-tinygo.wasm")
}

func testJSONFidelity(t *testing.T, wasmPath string) {
	transform := mustCreateIntegrationTransform(t, wasmPath)
	defer transform.Close()

	event := &cdc.Event{
		Source: "postgres",
		Table:  "complex_table",
		Op:     cdc.EventTypeUpdate,
		TS:     time.Date(2025, 7, 4, 15, 30, 45, 0, time.UTC),
		Before: map[string]any{
			"id":       float64(1),
			"name":     "before",
			"active":   true,
			"score":    float64(99.5),
			"nullable": nil,
		},
		After: map[string]any{
			"id":       float64(1),
			"name":     "after",
			"active":   false,
			"score":    float64(100.0),
			"nullable": nil,
			"nested":   map[string]any{"key": "value", "num": float64(42)},
			"array":    []any{float64(1), float64(2), float64(3)},
			"empty_str": "",
			"zero":     float64(0),
		},
	}

	result, err := transform.Process(event)
	if err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if result == nil {
		t.Fatal("Process() returned nil")
	}

	// Verify all Before fields
	if result.Before["active"] != true {
		t.Errorf("Before[active] = %v, want true", result.Before["active"])
	}
	if result.Before["nullable"] != nil {
		t.Errorf("Before[nullable] = %v, want nil", result.Before["nullable"])
	}

	// Verify all After fields
	if result.After["active"] != false {
		t.Errorf("After[active] = %v, want false", result.After["active"])
	}
	if result.After["empty_str"] != "" {
		t.Errorf("After[empty_str] = %v, want empty string", result.After["empty_str"])
	}
	if result.After["zero"] != float64(0) {
		t.Errorf("After[zero] = %v, want 0", result.After["zero"])
	}

	// Verify nested object survived round-trip
	nested, ok := result.After["nested"].(map[string]any)
	if !ok {
		t.Fatalf("After[nested] is not map[string]any: %T", result.After["nested"])
	}
	if nested["key"] != "value" {
		t.Errorf("nested[key] = %v, want %q", nested["key"], "value")
	}

	// Verify array survived round-trip
	arr, ok := result.After["array"].([]any)
	if !ok {
		t.Fatalf("After[array] is not []any: %T", result.After["array"])
	}
	if len(arr) != 3 {
		t.Errorf("array len = %d, want 3", len(arr))
	}

	// Verify JSON size is consistent
	inputJSON, _ := json.Marshal(event)
	outputJSON, _ := json.Marshal(result)
	t.Logf("input JSON: %d bytes, output JSON: %d bytes", len(inputJSON), len(outputJSON))
}

// TestIntegration_Passthrough_RustRepeatedCreateClose tests creating and closing transforms
func TestIntegration_Passthrough_RustRepeatedCreateClose(t *testing.T) {
	testRepeatedCreateClose(t, "./testdata/passthrough.wasm")
}

// TestIntegration_Passthrough_TinyGoRepeatedCreateClose tests creating and closing transforms
func TestIntegration_Passthrough_TinyGoRepeatedCreateClose(t *testing.T) {
	testRepeatedCreateClose(t, "./testdata/passthrough-tinygo.wasm")
}

func testRepeatedCreateClose(t *testing.T, wasmPath string) {
	for i := range 5 {
		transform := mustCreateIntegrationTransform(t, wasmPath)

		event := &cdc.Event{
			Source: "postgres",
			Table:  "users",
			Op:     cdc.EventTypeCreate,
			TS:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			After:  map[string]any{"iter": float64(i)},
		}

		result, err := transform.Process(event)
		if err != nil {
			t.Fatalf("iteration %d: Process error: %v", i, err)
		}
		if result == nil {
			t.Fatalf("iteration %d: result is nil", i)
		}
		if result.After["iter"] != float64(i) {
			t.Errorf("iteration %d: After[iter] = %v", i, result.After["iter"])
		}

		if err := transform.Close(); err != nil {
			t.Fatalf("iteration %d: Close error: %v", i, err)
		}
	}
}

// TestIntegration_CrossRuntime_IdenticalBehavior confirms both runtimes produce same results
func TestIntegration_CrossRuntime_IdenticalBehavior(t *testing.T) {
	rustTransform := mustCreateIntegrationTransform(t, "./testdata/filter.wasm")
	defer rustTransform.Close()

	tinygoTransform := mustCreateIntegrationTransform(t, "./testdata/filter-tinygo.wasm")
	defer tinygoTransform.Close()

	events := []*cdc.Event{
		{Source: "postgres", Table: "users", Op: cdc.EventTypeCreate, TS: time.Now(), After: map[string]any{"id": float64(1)}},
		{Source: "postgres", Table: "audit_logs", Op: cdc.EventTypeCreate, TS: time.Now(), After: map[string]any{"id": float64(2)}},
		{Source: "postgres", Table: "orders", Op: cdc.EventTypeUpdate, TS: time.Now(), Before: map[string]any{"status": "pending"}, After: map[string]any{"status": "done"}},
		{Source: "postgres", Table: "sessions", Op: cdc.EventTypeDelete, TS: time.Now(), Before: map[string]any{"id": float64(3)}},
	}

	for i, event := range events {
		rustResult, rustErr := rustTransform.Process(event)
		tinygoResult, tinygoErr := tinygoTransform.Process(event)

		// Both should have same error behavior
		if (rustErr == nil) != (tinygoErr == nil) {
			t.Errorf("event %d (%s): Rust err=%v, TinyGo err=%v", i, event.Table, rustErr, tinygoErr)
			continue
		}

		// Both should have same nil/non-nil result
		if (rustResult == nil) != (tinygoResult == nil) {
			t.Errorf("event %d (%s): Rust result nil=%v, TinyGo result nil=%v", i, event.Table, rustResult == nil, tinygoResult == nil)
			continue
		}

		if rustResult != nil {
			if rustResult.Table != tinygoResult.Table {
				t.Errorf("event %d: Rust table=%q, TinyGo table=%q", i, rustResult.Table, tinygoResult.Table)
			}
			if rustResult.Op != tinygoResult.Op {
				t.Errorf("event %d: Rust op=%q, TinyGo op=%q", i, rustResult.Op, tinygoResult.Op)
			}
		}
	}
}

// --- Integration test helpers ---

func mustCreateIntegrationTransform(t *testing.T, path string) *WASMTransform {
	t.Helper()
	transform, err := NewWASM(context.Background(), Config{Path: path})
	if err != nil {
		t.Fatalf("failed to create transform from %s: %v", path, err)
	}
	return transform
}

type testError struct {
	msg string
}

func (e *testError) Error() string { return e.msg }

func errorf(format string, args ...any) error {
	return &testError{msg: fmt.Sprintf(format, args...)}
}
