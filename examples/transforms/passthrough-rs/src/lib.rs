//! Passthrough WASM transform — returns CDC events unchanged.
//!
//! Build: cargo build --target wasm32-unknown-unknown --release
//!
//! ABI:
//!   alloc(size: u32) -> u32      — bump allocator
//!   handle(ptr: u32, len: u32) -> u32  — returns pointer to WASMResult
//!
//! WASMResult layout (16 bytes LE): [data_ptr:u32][data_len:u32][err_ptr:u32][err_len:u32]

use std::alloc::{alloc as heap_alloc, Layout};

/// WASMResult returned to the host.
#[repr(C)]
struct WasmResult {
    data_ptr: u32,
    data_len: u32,
    err_ptr: u32,
    err_len: u32,
}

/// Bump allocator for host to write input data.
#[unsafe(no_mangle)]
pub extern "C" fn alloc(size: u32) -> u32 {
    let layout = Layout::from_size_align(size as usize, 1).unwrap();
    let ptr = unsafe { heap_alloc(layout) };
    ptr as u32
}

/// Passthrough handler — copies input and returns it unchanged.
#[unsafe(no_mangle)]
pub extern "C" fn handle(ptr: u32, len: u32) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };

    // Copy input data
    let data_ptr = alloc(len);
    let dst = unsafe { std::slice::from_raw_parts_mut(data_ptr as *mut u8, len as usize) };
    dst.copy_from_slice(input);

    // Allocate and write result struct
    let result_size = std::mem::size_of::<WasmResult>() as u32;
    let result_ptr = alloc(result_size);
    let result = unsafe { &mut *(result_ptr as *mut WasmResult) };
    result.data_ptr = data_ptr;
    result.data_len = len;
    result.err_ptr = 0;
    result.err_len = 0;

    result_ptr
}
