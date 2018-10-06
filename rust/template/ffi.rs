#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals, unused_parens, non_shorthand_field_patterns, dead_code)]

use std::sync::{Arc,Mutex};
use std::slice;
use std::ops::Deref;
use libc::*;
use super::*;
use differential_datalog::arcval;

use std::ffi::{CStr,CString};
use std::os::raw::c_char;

trait ToFFI {
    type FFIType;

    fn to_ffi(&self) -> Self::FFIType;
    // Takes a reference to FFI object generated by ToFFI::to_ffi
    // method and deallocates its heap-allocated content
    fn free(&mut Self::FFIType) {}

    // Generate C code that creates a C representation of the value
    // (used for testing the FFI interface)
    fn c_code(&self) -> String;
}

impl ToFFI for bool {
    type FFIType = bool;

    fn to_ffi(&self) -> Self::FFIType {
        *self
    }

    fn c_code(&self) -> String {
        match self {
            &true  => "true".to_string(),
            &false => "false".to_string()
        }
    }
}

impl ToFFI for u8 {
    type FFIType = uint8_t;

    fn to_ffi(&self) -> Self::FFIType {
        *self as Self::FFIType
    }
    fn c_code(&self) -> String {
        format!("{}", *self)
    }
}

impl ToFFI for u16 {
    type FFIType = uint16_t;

    fn to_ffi(&self) -> Self::FFIType {
        *self as Self::FFIType
    }
    fn c_code(&self) -> String {
        format!("{}", *self)
    }
}

impl ToFFI for u32 {
    type FFIType = uint32_t;

    fn to_ffi(&self) -> Self::FFIType {
        *self as Self::FFIType
    }

    fn c_code(&self) -> String {
        format!("{}", *self)
    }

}

impl ToFFI for u64 {
    type FFIType = uint64_t;

    fn to_ffi(&self) -> Self::FFIType {
        *self as Self::FFIType
    }

    fn c_code(&self) -> String {
        format!("{}", *self)
    }
}

#[repr(C)]
pub struct Uint128_le_t {
    word0: uint64_t,
    word1: uint64_t
}

impl Uint128_le_t {
    pub fn to_u128(&self) -> u128 {
        (self.word0 as u128) | ((self.word1 as u128) << 64)
    }
}

impl ToFFI for u128 {
    type FFIType = Uint128_le_t;

    fn to_ffi(&self) -> Self::FFIType {
        Uint128_le_t{
            word0: ((*self) & 0xffffffffffffffff) as uint64_t,
            word1: ((*self) >> 64) as uint64_t
        }
    }

    fn c_code(&self) -> String {
        format!("(struct Uint128_le_t){{(__uint64_t)0x{:x}, (__uint64_t)0x{:x}}}", (*self) & 0xffffffffffffffff, (*self) >> 64)
    }
}

impl ToFFI for String {
    type FFIType = *mut c_char;

    fn to_ffi(&self) -> Self::FFIType {
        CString::new(self.clone()).unwrap().into_raw()
    }

    fn free(x: &mut Self::FFIType) {
        unsafe { CString::from_raw(*x); }
    }

    fn c_code(&self) -> String {
        format!("{:?}", *self)
    }
}

impl<T:Val+ToFFI> ToFFI for arcval::ArcVal<T> {
    type FFIType = T::FFIType;

    fn to_ffi(&self) -> Self::FFIType {
        self.deref().to_ffi()
    }

    fn free(x: &mut Self::FFIType) {
        T::free(x)
    }

    fn c_code(&self) -> String {
        self.deref().c_code()
    }
}

impl ToFFI for Uint {
    type FFIType = *mut Uint;

    fn to_ffi(&self) -> Self::FFIType {
        Box::into_raw(Box::new(self.clone()))
    }
    fn free(x: &mut Self::FFIType) {
        unsafe { Box::from_raw(*x); }
    }
    fn c_code(&self) -> String {
        format!("uint_from_str(\"{}\", 10)", *self)
    }
}

impl ToFFI for Int {
    type FFIType = *mut Int;

    fn to_ffi(&self) -> Self::FFIType {
        Box::into_raw(Box::new(self.clone()))
    }
    fn free(x: &mut Self::FFIType) {
        unsafe { Box::from_raw(*x); }
    }
    fn c_code(&self) -> String {
        format!("int_from_str(\"{}\", 10)", *self)
    }
}

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
