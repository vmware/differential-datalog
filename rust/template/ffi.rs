use differential_datalog::program::*;

use std::sync::{Arc,Mutex};
use std::slice;
use libc::*;
use super::*;

use std::ffi::CStr;
use std::os::raw::c_char;
use differential_datalog::program::*;
use differential_datalog::uint::*;
use differential_datalog::int::*;


type CUpdateCallback = extern fn(relid: size_t, val: *const __c_Value, pol: bool);

#[no_mangle]
pub extern "C" fn datalog_example_run(upd_cb: CUpdateCallback) -> *mut Arc<Mutex<RunningProgram<Value>>> {
    let p = prog(Arc::new(move |relid, val, pol|upd_cb(relid as size_t, Box::into_raw(Box::new(__c_Value::from_native(val))), pol)));
    let running = Box::new(Arc::new(Mutex::new(p.run(1))));
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
    match prog.delete(relid as RelId, v.to_native()) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Datalog runtime error: {}", e.err);
            -1
        }
    }
}


#[repr(C)]
pub struct __c_Update {
    pol: bool,
    v: *const __c_Value
}

#[no_mangle]
pub extern "C" fn datalog_example_apply_updates(prog: *mut RunningProgram<Value>,
                                                updates: * const __c_Update,
                                                nupdates: size_t) -> c_int {
    let prog = unsafe { &mut *prog };
    let updates = unsafe {slice::from_raw_parts(updates, nupdates as usize)};
    let updates = updates.iter().map(|&__c_Update{pol,v}| 
                                     {
                                         let v = unsafe{&*v};
                                         if pol {
                                             Update::Insert{relid: (*v).tag as usize, v: v.to_native()}
                                         } else {
                                             Update::Delete{relid: (*v).tag as usize, v: v.to_native()}
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
