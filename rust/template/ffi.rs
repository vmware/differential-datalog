#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals, unused_parens, non_shorthand_field_patterns, dead_code)]

use std::sync::{Arc,Mutex};
use std::slice;
use std::ops::Deref;
use libc::*;
use super::*;
use super::to_ffi::*;
use differential_datalog::arcval;

use std::ffi::{CStr,CString};
use std::os::raw::c_char;

type CUpdateCallback = extern "C" fn(ctx: uintptr_t, relid: size_t, val: *mut __c_Value, pol: bool);

#[no_mangle]
pub extern "C" fn datalog_example_run(upd_cb: CUpdateCallback, ctx: uintptr_t) -> *mut RunningProgram<Value> {
    let p = prog(Arc::new(move |relid, val, pol|
                          __c_Value::from_val(relid, val).map_or((), |v| upd_cb(ctx, relid as size_t, Box::into_raw(Box::new(v)), pol)))
                 );
    let running = Box::new(p.run(1));
    Box::into_raw(running)
}

#[no_mangle]
pub extern "C" fn datalog_example_stop(prog: *mut RunningProgram<Value>) -> c_int {
    let prog = unsafe { Box::from_raw(prog) };
    match prog.stop() {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Datalog runtime error: {}", e.err);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn datalog_example_transaction_start(prog: *mut RunningProgram<Value>) -> c_int {
    let prog = unsafe { &mut *prog };
    match prog.transaction_start() {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Datalog runtime error: {}", e.err);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn datalog_example_transaction_commit(prog: *mut RunningProgram<Value>) -> c_int {
    let prog = unsafe { &mut *prog };
    match prog.transaction_commit() {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Datalog runtime error: {}", e.err);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn datalog_example_transaction_rollback(prog: *mut RunningProgram<Value>) -> c_int {
    let prog = unsafe { &mut *prog };
    match prog.transaction_rollback() {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Datalog runtime error: {}", e.err);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn datalog_example_insert(prog: *mut RunningProgram<Value>, relid: size_t, v: *const __c_Value) -> c_int {
    let prog = unsafe { &mut *prog };
    let v = unsafe { &*v };
    match prog.insert(relid as RelId, v.to_native()) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Datalog runtime error: {}", e.err);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn datalog_example_delete(prog: *mut RunningProgram<Value>, relid: size_t, v: *const __c_Value) -> c_int {
    let prog = unsafe { &mut *prog };
    let v = unsafe { &*v };
    match prog.delete_value(relid as RelId, v.to_native()) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Datalog runtime error: {}", e.err);
            -1
        }
    }
}

#[repr(C)]
pub enum __c_UpdateOp {
    UpdateInsert,
    UpdateDelete,
    UpdateDeleteKey
}

#[repr(C)]
pub struct __c_Update {
    op: __c_UpdateOp,
    v: *const __c_Value
}

#[no_mangle]
pub extern "C" fn datalog_example_apply_updates(prog: *mut RunningProgram<Value>,
                                                updates: * const __c_Update,
                                                nupdates: size_t) -> c_int {
    let prog = unsafe { &mut *prog };
    let updates = unsafe {slice::from_raw_parts(updates, nupdates as usize)};
    let updates = updates.iter().map(|&__c_Update{op,v}|
                                     {
                                         let v = unsafe{&*v};
                                         match op {
                                             __c_UpdateOp::UpdateInsert     => Update::Insert{relid: (*v).tag as usize, v: v.to_native()},
                                             __c_UpdateOp::UpdateDelete     => Update::DeleteValue{relid: (*v).tag as usize, v: v.to_native()},
                                             __c_UpdateOp::UpdateDeleteKey  => Update::DeleteKey{relid: (*v).tag as usize, v: v.to_native()}
                                         }
                                     }).collect();
    match prog.apply_updates(updates) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Datalog runtime error: {}", e.err);
            -1
        }
    }
}
