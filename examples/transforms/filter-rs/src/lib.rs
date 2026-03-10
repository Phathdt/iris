//! Table filter WASM transform — only allows events from specified tables.
//!
//! Build: cargo build --target wasm32-unknown-unknown --release
//!
//! Behavior:
//!   - Events from "users" or "orders" tables pass through unchanged
//!   - All other tables are dropped (data_len=0)

use serde::Deserialize;
use std::alloc::{alloc as heap_alloc, Layout};

#[repr(C)]
struct WasmResult {
    data_ptr: u32,
    data_len: u32,
    err_ptr: u32,
    err_len: u32,
}

/// Minimal CDC event — only parse fields needed for filtering.
#[derive(Deserialize)]
struct CdcEvent {
    table: String,
}

const ALLOWED_TABLES: &[&str] = &["users", "orders"];

/// Bump allocator. Returns 0 on failure.
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

#[no_mangle]
pub extern "C" fn handle(ptr: u32, len: u32) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };

    // Parse just the table field
    let event: CdcEvent = match serde_json::from_slice(input) {
        Ok(e) => e,
        Err(e) => return write_error(&format!("parse error: {e}")),
    };

    // Filter: drop if table not in allowed list
    if !ALLOWED_TABLES.contains(&event.table.as_str()) {
        return write_drop();
    }

    // Passthrough: return original input
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

fn write_drop() -> u32 {
    write_result(0, 0, 0, 0)
}

fn write_error(msg: &str) -> u32 {
    let bytes = msg.as_bytes();
    let err_ptr = alloc(bytes.len() as u32);
    if err_ptr == 0 {
        return 0;
    }
    let dst = unsafe { std::slice::from_raw_parts_mut(err_ptr as *mut u8, bytes.len()) };
    dst.copy_from_slice(bytes);
    write_result(0, 0, err_ptr, bytes.len() as u32)
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
