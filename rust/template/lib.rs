#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals, unused_parens, non_shorthand_field_patterns, dead_code)]

extern crate fnv;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate libc;
extern crate twox_hash;

extern crate differential_datalog;

#[macro_use]
extern crate abomonation;
extern crate ddlog_ovsdb_adapter;

use differential_datalog::program::*;
use differential_datalog::uint::*;
use differential_datalog::int::*;
use differential_datalog::arcval;
use differential_datalog::record::*;
use abomonation::Abomonation;

use fnv::FnvHashSet;
use fnv::FnvHashMap;
use std::fmt::Display;
use std::fmt;
use std::sync;
use std::hash::Hash;
use std::hash::Hasher;
use std::os::raw;

pub mod valmap;
pub mod ovsdb;
mod stdlib;

use self::stdlib::*;

pub fn updcmd2upd(c: &UpdCmd) -> Result<Update<Value>, String> {
    match c {
        UpdCmd::Insert(rname, rec) => {
            let relid: Relations = relname2id(rname).ok_or(format!("Unknown relation {}", rname))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::Insert{relid: relid as RelId, v: val})
        },
        UpdCmd::Delete(rname, rec) => {
            let relid: Relations = relname2id(rname).ok_or(format!("Unknown relation {}", rname))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::DeleteValue{relid: relid as RelId, v: val})
        },
        UpdCmd::DeleteKey(rname, rec) => {
            let relid: Relations = relname2id(rname).ok_or(format!("Unknown relation {}", rname))?;
            let key = relkey_from_record(relid, rec)?;
            Ok(Update::DeleteKey{relid: relid as RelId, k: key})
        }
    }
}


fn dummy_cb(_relid: RelId, _v: &Value, _pol: bool) {}

#[no_mangle]
pub extern "C" fn datalog_example_run(workers: raw::c_uint) -> *const sync::Mutex<RunningProgram<Value>> {
    let program = prog(sync::Arc::new(dummy_cb));
    sync::Arc::into_raw(sync::Arc::new(sync::Mutex::new(program.run(workers as usize))))
}

#[no_mangle]
pub extern "C" fn datalog_example_stop(prog: *const sync::Mutex<RunningProgram<Value>>) -> raw::c_int {
    let prog = unsafe {sync::Arc::from_raw(prog)};
    match sync::Arc::try_unwrap(prog) {
        Ok(prog) => prog.into_inner().map(|p|{p.stop(); 0}).unwrap_or_else(|e|{
            eprintln!("datalog_example_stop(): error acquiring lock: {}", e);
            -1
        }),
        Err(e) => {
            eprintln!("datalog_example_stop(): cannot extract value from Arc");
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn datalog_example_transaction_start(prog: *const sync::Mutex<RunningProgram<Value>>) -> raw::c_int {
    let prog = unsafe {sync::Arc::from_raw(prog)};
    let res = prog.lock().unwrap().transaction_start().map(|_|0).unwrap_or_else(|e|{
        eprintln!("datalog_example_transaction_start(): error: {}", e);
        -1
    });
    sync::Arc::into_raw(prog);
    res
}

#[no_mangle]
pub extern "C" fn datalog_example_transaction_commit(prog: *const sync::Mutex<RunningProgram<Value>>) -> raw::c_int {
    let prog = unsafe {sync::Arc::from_raw(prog)};
    let res = prog.lock().unwrap().transaction_commit().map(|_|0).unwrap_or_else(|e|{
        eprintln!("datalog_example_transaction_commit(): error: {}", e);
        -1
    });
    sync::Arc::into_raw(prog);
    res
}

#[no_mangle]
pub extern "C" fn datalog_example_transaction_rollback(prog: *const sync::Mutex<RunningProgram<Value>>) -> raw::c_int {
    let prog = unsafe {sync::Arc::from_raw(prog)};
    let res = prog.lock().unwrap().transaction_rollback().map(|_|0).unwrap_or_else(|e|{
        eprintln!("datalog_example_transaction_rollback(): error: {}", e);
        -1
    });
    sync::Arc::into_raw(prog);
    res
}
