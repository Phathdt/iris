package wasmtransform

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tetratelabs/wazero/api"
)

// Memory provides helpers for WASM memory allocation and data transfer.
type Memory struct {
	module api.Module
}

// NewMemory creates a new Memory helper.
func NewMemory() *Memory {
	return &Memory{}
}

// Alloc allocates memory in the WASM module and returns the pointer.
// It calls the exported 'alloc' function with the size in bytes.
func Alloc(ctx context.Context, mod api.Module, allocFn api.Function, size uint32) (uint32, error) {
	if allocFn == nil {
		return 0, errors.New("alloc function not found")
	}

	results, err := allocFn.Call(ctx, uint64(size))
	if err != nil {
		return 0, fmt.Errorf("failed to call alloc: %w", err)
	}

	// wazero returns []uint64, take first result
	ptr := uint32(results[0])
	if ptr == 0 {
		return 0, errors.New("alloc returned null pointer")
	}

	return ptr, nil
}

// WriteToMemory writes data to WASM memory at the given pointer.
func WriteToMemory(mod api.Module, ptr uint32, data []byte) error {
	mem := mod.Memory()
	if mem == nil {
		return errors.New("module memory not found")
	}

	if !mem.Write(ptr, data) {
		return fmt.Errorf("failed to write %d bytes at offset %d", len(data), ptr)
	}

	return nil
}

// ReadFromMemory reads data from WASM memory at the given pointer.
func ReadFromMemory(mod api.Module, ptr uint32, size uint32) ([]byte, error) {
	mem := mod.Memory()
	if mem == nil {
		return nil, errors.New("module memory not found")
	}

	data, ok := mem.Read(ptr, size)
	if !ok {
		return nil, fmt.Errorf("failed to read %d bytes at offset %d", size, ptr)
	}

	// Return a copy to avoid memory aliasing issues
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

// WASMResult is the expected output structure from a WASM transform function.
// Layout: [ptr:4bytes][len:4bytes][errPtr:4bytes][errLen:4bytes]
type WASMResult struct {
	DataPtr  uint32
	DataLen  uint32
	ErrPtr   uint32
	ErrLen   uint32
}

// ParseResult reads the result structure from WASM memory.
// The result layout is: [data_ptr:u32][data_len:u32][err_ptr:u32][err_len:u32]
func ParseResult(mod api.Module, resultPtr uint32) (*WASMResult, error) {
	mem := mod.Memory()
	if mem == nil {
		return nil, errors.New("module memory not found")
	}

	// Read 16 bytes: 4 x uint32
	data, ok := mem.Read(resultPtr, 16)
	if !ok {
		return nil, fmt.Errorf("failed to read result at offset %d", resultPtr)
	}

	return &WASMResult{
		DataPtr: binary.LittleEndian.Uint32(data[0:4]),
		DataLen: binary.LittleEndian.Uint32(data[4:8]),
		ErrPtr:  binary.LittleEndian.Uint32(data[8:12]),
		ErrLen:  binary.LittleEndian.Uint32(data[12:16]),
	}, nil
}

// Free calls the exported 'free' function if available.
func Free(ctx context.Context, mod api.Module, freeFn api.Function, ptr uint32) error {
	if freeFn == nil {
		return nil // free is optional
	}

	_, err := freeFn.Call(ctx, uint64(ptr))
	return err
}
