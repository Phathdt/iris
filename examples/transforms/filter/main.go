// Package main implements a CDC event filter as a WASM module.
//
// Build with TinyGo:
//
//	tinygo build -o filter.wasm -target=wasi -no-debug main.go
//
// Behavior:
//   - Events from "users" or "orders" tables pass through unchanged
//   - All other tables are dropped (data_len=0)
package main

import (
	"encoding/json"
	"unsafe"
)

var heap = make([]byte, 0, 64*1024)

//export alloc
func alloc(size uint32) uint32 {
	pos := len(heap)
	if pos+int(size) > cap(heap) {
		return 0
	}
	heap = heap[:pos+int(size)]
	return uint32(uintptr(unsafe.Pointer(&heap[pos])))
}

type wasmResult struct {
	dataPtr uint32
	dataLen uint32
	errPtr  uint32
	errLen  uint32
}

// eventView contains only the fields needed for filtering.
type eventView struct {
	Table string `json:"table"`
}

var allowedTables = map[string]bool{
	"users":  true,
	"orders": true,
}

//export handle
func handle(ptr, size uint32) uint32 {
	input := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), size)

	var event eventView
	if err := json.Unmarshal(input, &event); err != nil {
		return writeError("unmarshal: " + err.Error())
	}

	// Filter: drop events from tables not in the allow list
	if !allowedTables[event.Table] {
		return writeDrop()
	}

	// Passthrough: return the original event unchanged
	return writeData(input)
}

func writeData(data []byte) uint32 {
	dataPtr := alloc(uint32(len(data)))
	if dataPtr == 0 {
		return 0
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(dataPtr))), len(data))
	copy(dst, data)
	return writeResult(dataPtr, uint32(len(data)), 0, 0)
}

func writeDrop() uint32 {
	return writeResult(0, 0, 0, 0)
}

func writeError(msg string) uint32 {
	errBytes := []byte(msg)
	errPtr := alloc(uint32(len(errBytes)))
	if errPtr == 0 {
		return 0
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(errPtr))), len(errBytes))
	copy(dst, errBytes)
	return writeResult(0, 0, errPtr, uint32(len(errBytes)))
}

func writeResult(dataPtr, dataLen, errPtr, errLen uint32) uint32 {
	resultPtr := alloc(uint32(unsafe.Sizeof(wasmResult{})))
	if resultPtr == 0 {
		return 0
	}
	result := (*wasmResult)(unsafe.Pointer(uintptr(resultPtr)))
	result.dataPtr = dataPtr
	result.dataLen = dataLen
	result.errPtr = errPtr
	result.errLen = errLen
	return resultPtr
}

func main() {}
