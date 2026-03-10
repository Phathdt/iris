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
/// Returns 0 on allocation failure.
#[no_mangle]
pub extern "C" fn alloc(size: u32) -> u32 {
    let layout = match Layout::from_size_align(size as usize, 1) {
        Ok(l) => l,
        Err(_) => return 0,
    };
    let ptr = unsafe { heap_alloc(layout) };
    if ptr.is_null() {
        return 0;
    }
    ptr as u32
}

/// Passthrough handler — copies input and returns it unchanged.
#[no_mangle]
pub extern "C" fn handle(ptr: u32, len: u32) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    write_data(input)
}

fn write_data(data: &[u8]) -> u32 {
    let data_ptr = alloc(data.len() as u32);
    if data_ptr == 0 {
        return 0;
    }
    let dst = unsafe { std::slice::from_raw_parts_mut(data_ptr as *mut u8, data.len()) };
    dst.copy_from_slice(data);
    write_result(data_ptr, data.len() as u32, 0, 0)
}

fn write_result(data_ptr: u32, data_len: u32, err_ptr: u32, err_len: u32) -> u32 {
    let result_size = std::mem::size_of::<WasmResult>() as u32;
    let result_ptr = alloc(result_size);
    if result_ptr == 0 {
        return 0;
    }
    let result = unsafe { &mut *(result_ptr as *mut WasmResult) };
    result.data_ptr = data_ptr;
    result.data_len = data_len;
    result.err_ptr = err_ptr;
    result.err_len = err_len;
    result_ptr
}
