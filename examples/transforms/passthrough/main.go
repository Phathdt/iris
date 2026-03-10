// Package main implements a passthrough WASM module that returns events unchanged.
//
// Build with TinyGo:
//
//	tinygo build -o passthrough.wasm -target=wasi -no-debug main.go
package main

import "unsafe"

var heap = make([]byte, 0, 64*1024)

//export alloc
func alloc(size uint32) uint32 {
	pos := len(heap)
	heap = heap[:pos+int(size)]
	return uint32(uintptr(unsafe.Pointer(&heap[pos])))
}

type wasmResult struct {
	dataPtr uint32
	dataLen uint32
	errPtr  uint32
	errLen  uint32
}

//export handle
func handle(ptr, size uint32) uint32 {
	input := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), size)

	// Copy input to our heap
	dataPtr := alloc(uint32(len(input)))
	dst := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(dataPtr))), len(input))
	copy(dst, input)

	// Write result struct
	resultPtr := alloc(uint32(unsafe.Sizeof(wasmResult{})))
	result := (*wasmResult)(unsafe.Pointer(uintptr(resultPtr)))
	result.dataPtr = dataPtr
	result.dataLen = uint32(len(input))
	result.errPtr = 0
	result.errLen = 0
	return resultPtr
}

func main() {}
