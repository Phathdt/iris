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

//export handle
func handle(ptr, size uint32) uint32 {
	input := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), size)
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
