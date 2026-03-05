package wasmtransform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"iris/pkg/cdc"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// WASMTransform runs WebAssembly modules to transform CDC events.
// Uses wazero runtime (pure Go, no CGO).
type WASMTransform struct {
	runtime      wazero.Runtime
	compiled     wazero.CompiledModule
	config       Config
	allocFnName  string
	handleFnName string
}

// compileWASM loads and compiles the WASM module.
func compileWASM(ctx context.Context, config Config) (wazero.Runtime, wazero.CompiledModule, error) {
	if config.Path == "" {
		return nil, nil, errors.New("WASM module path is required")
	}

	// Create runtime with WASI support
	runtime := wazero.NewRuntime(ctx)

	// Instantiate WASI if the module needs it
	wasi_snapshot_preview1.Instantiate(ctx, runtime)

	// Load WASM file
	wasmBytes, err := os.ReadFile(config.Path)
	if err != nil {
		runtime.Close(ctx)
		return nil, nil, fmt.Errorf("failed to read WASM file: %w", err)
	}

	// Compile the module
	compiled, err := runtime.CompileModule(ctx, wasmBytes)
	if err != nil {
		runtime.Close(ctx)
		return nil, nil, fmt.Errorf("failed to compile WASM module: %w", err)
	}

	return runtime, compiled, nil
}

// NewWASM creates a new WASM-based transform.
func NewWASM(ctx context.Context, config Config) (*WASMTransform, error) {
	runtime, compiled, err := compileWASM(ctx, config)
	if err != nil {
		return nil, err
	}

	transform := &WASMTransform{
		runtime:      runtime,
		compiled:     compiled,
		config:       config,
		allocFnName:  config.AllocFunctionName,
		handleFnName: config.FunctionName,
	}

	// Apply defaults if not set
	if transform.allocFnName == "" {
		transform.allocFnName = "alloc"
	}
	if transform.handleFnName == "" {
		transform.handleFnName = "handle"
	}

	return transform, nil
}

// Process transforms the event using the WASM module.
// The WASM module receives JSON bytes and returns transformed JSON or an error.
func (t *WASMTransform) Process(event *cdc.Event) (*cdc.Event, error) {
	ctx := context.Background()

	// Instantiate a fresh module instance for each call
	// This provides isolation and avoids state leakage
	mod, err := t.runtime.InstantiateModule(ctx, t.compiled, wazero.NewModuleConfig().WithName(""))
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate WASM module: %w", err)
	}
	defer mod.Close(ctx)

	// Get the alloc function
	allocFn := mod.ExportedFunction(t.allocFnName)
	if allocFn == nil {
		return nil, fmt.Errorf("exported function '%s' not found in WASM module", t.allocFnName)
	}

	// Get the handle function
	handleFn := mod.ExportedFunction(t.handleFnName)
	if handleFn == nil {
		return nil, fmt.Errorf("exported function '%s' not found in WASM module", t.handleFnName)
	}

	// Marshal event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	// Allocate memory for input
	inputPtr, err := Alloc(ctx, mod, allocFn, uint32(len(eventJSON)))
	if err != nil {
		return nil, err
	}

	// Write JSON to WASM memory
	if err := WriteToMemory(mod, inputPtr, eventJSON); err != nil {
		return nil, err
	}

	// Call handle(input_ptr, input_len)
	// Expected return: pointer to wasmResult struct
	results, err := handleFn.Call(ctx, uint64(inputPtr), uint64(len(eventJSON)))
	if err != nil {
		return nil, fmt.Errorf("WASM handle function failed: %w", err)
	}

	// Parse result pointer (first return value)
	resultPtr := uint32(results[0])
	if resultPtr == 0 {
		return nil, errors.New("WASM handle returned null pointer")
	}

	// Read the result structure from memory
	wasmResult, err := ParseResult(mod, resultPtr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WASM result: %w", err)
	}

	// Check for error from WASM
	if wasmResult.ErrLen > 0 {
		errData, err := ReadFromMemory(mod, wasmResult.ErrPtr, wasmResult.ErrLen)
		if err == nil && len(errData) > 0 {
			return nil, fmt.Errorf("WASM transform error: %s", string(errData))
		}
		return nil, errors.New("WASM transform returned error")
	}

	// Read transformed data
	if wasmResult.DataLen == 0 {
		// Empty data means drop the event
		return nil, nil
	}

	transformedData, err := ReadFromMemory(mod, wasmResult.DataPtr, wasmResult.DataLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read transformed data: %w", err)
	}

	// Unmarshal transformed JSON back to Event
	var transformedEvent cdc.Event
	if err := json.Unmarshal(transformedData, &transformedEvent); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transformed event: %w", err)
	}

	return &transformedEvent, nil
}

// Close releases the WASM runtime resources.
func (t *WASMTransform) Close() error {
	return t.runtime.Close(context.Background())
}

// Ensure WASMTransform implements cdc.Transform.
var _ cdc.Transform = (*WASMTransform)(nil)
