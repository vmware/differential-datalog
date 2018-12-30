#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals, unused_parens, non_shorthand_field_patterns, dead_code)]

extern crate fnv;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate libc;
extern crate twox_hash;

#[macro_use]
extern crate differential_datalog;

#[macro_use]
extern crate abomonation;
extern crate ddlog_ovsdb_adapter;

use differential_datalog::program::*;
use differential_datalog::uint::*;
use differential_datalog::int::*;
use differential_datalog::arcval;
use differential_datalog::record;
use differential_datalog::record::{FromRecord, IntoRecord};
use abomonation::Abomonation;

use fnv::{FnvHashSet, FnvHashMap};
use std::fmt::Display;
use std::fmt;
use std::sync;
use std::hash::Hash;
use std::hash::Hasher;
use std::os::raw;
use std::borrow;
use std::ptr;
use std::ffi;
use std::boxed;
use std::ops::Deref;
use libc::size_t;

pub mod valmap;
pub mod ovsdb;

pub type HDDlog = (sync::Mutex<RunningProgram<Value>>, Option<sync::Arc<sync::Mutex<valmap::ValMap>>>);

pub fn updcmd2upd(c: &record::UpdCmd) -> Result<Update<Value>, String> {
    match c {
        record::UpdCmd::Insert(rident, rec) => {
            let relid: Relations = relident2id(rident).ok_or_else(||format!("Unknown relation {}", rident))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::Insert{relid: relid as RelId, v: val})
        },
        record::UpdCmd::Delete(rident, rec) => {
            let relid: Relations = relident2id(rident).ok_or_else(||format!("Unknown relation {}", rident))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::DeleteValue{relid: relid as RelId, v: val})
        },
        record::UpdCmd::DeleteKey(rident, rec) => {
            let relid: Relations = relident2id(rident).ok_or_else(||format!("Unknown relation {}", rident))?;
            let key = relkey_from_record(relid, rec)?;
            Ok(Update::DeleteKey{relid: relid as RelId, k: key})
        }
    }
}

fn relident2id(r: &record::RelIdentifier) -> Option<Relations> {
    match r {
        record::RelIdentifier::RelName(rname) => relname2id(rname),
        record::RelIdentifier::RelId(id)      => relid2rel(*id)
    }
}


fn __null_cb(_relid: RelId, _v: &Value, _w: isize) {}

fn upd_cb(db: &sync::Arc<sync::Mutex<valmap::ValMap>>, relid: RelId, v: &Value, pol: bool) {
    db.lock().unwrap().update(relid, v, pol);
}

#[no_mangle]
pub extern "C" fn ddlog_get_table_id(tname: *const raw::c_char) -> libc::size_t
{
    if tname.is_null() {
        return libc::size_t::max_value();
    };
    match get_table_id(tname) {
        Ok(relid) => relid as libc::size_t,
        Err(e) => {
            eprintln!("ddlog_get_table_id(): error: {}", e);
            libc::size_t::max_value()
        }
    }
}

fn get_table_id(tname: *const raw::c_char) -> Result<Relations, String>
{
    let table_str = unsafe{ ffi::CStr::from_ptr(tname) }.to_str().map_err(|e| format!("{}", e))?;
    relname2id(table_str).ok_or_else(||format!("unknown relation {}", table_str))
}

#[no_mangle]
pub extern "C" fn ddlog_run(workers: raw::c_uint,
                            do_store: bool,
                            cb: Option<extern "C" fn(arg: libc::uintptr_t,
                                                     table: libc::size_t,
                                                     rec: *const record::Record,
                                                     polarity: bool)>,
                            cb_arg:  libc::uintptr_t) -> *const HDDlog
{
    let workers = if workers == 0 { 1 } else { workers };
    if do_store {
        let db: sync::Arc<sync::Mutex<valmap::ValMap>> = sync::Arc::new(sync::Mutex::new(valmap::ValMap::new()));
        let db2 = db.clone();
        match cb {
            None => {
                let program = prog(sync::Arc::new(move |relid,v,w| {
                    debug_assert!(w == 1 || w == -1);
                    upd_cb(&db, relid, v, w == 1)
                }));
                let prog = program.run(workers as usize);
                sync::Arc::into_raw(sync::Arc::new((sync::Mutex::new(prog), Some(db2))))
            },
            Some(cb) => {
                let program = prog(sync::Arc::new(move |relid,v,w| {
                    debug_assert!(w == 1 || w == -1);
                    upd_cb(&db, relid, v, w == 1);
                    cb(cb_arg, relid, &v.clone().into_record() as *const record::Record, w == 1)
                }));
                let prog = program.run(workers as usize);
                sync::Arc::into_raw(sync::Arc::new((sync::Mutex::new(prog), Some(db2))))
            }
        }
    } else {
        match cb {
            None => {
                let program = prog(sync::Arc::new(|_,_,_| {}));
                let prog = program.run(workers as usize);
                sync::Arc::into_raw(sync::Arc::new((sync::Mutex::new(prog), None)))
            },
            Some(cb) => {
                let program = prog(sync::Arc::new(move |relid,v,w| {
                        debug_assert!(w == 1 || w == -1);
                        cb(cb_arg, relid, &v.clone().into_record() as *const record::Record, w == 1)
                    }));
                let prog = program.run(workers as usize);
                sync::Arc::into_raw(sync::Arc::new((sync::Mutex::new(prog), None)))
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_stop(prog: *const HDDlog) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = sync::Arc::from_raw(prog);
    match sync::Arc::try_unwrap(prog) {
        Ok((prog,_)) => prog.into_inner().map(|p|{p.stop(); 0}).unwrap_or_else(|e|{
            eprintln!("ddlog_stop(): error acquiring lock: {}", e);
            -1
        }),
        Err(e) => {
            eprintln!("ddlog_stop(): cannot extract value from Arc");
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_start(prog: *const HDDlog) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = sync::Arc::from_raw(prog);
    let res = prog.0.lock().unwrap().transaction_start().map(|_|0).unwrap_or_else(|e|{
        eprintln!("ddlog_transaction_start(): error: {}", e);
        -1
    });
    sync::Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit(prog: *const HDDlog) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = sync::Arc::from_raw(prog);
    let res = prog.0.lock().unwrap().transaction_commit().map(|_|0).unwrap_or_else(|e|{
        eprintln!("ddlog_transaction_commit(): error: {}", e);
        -1
    });
    sync::Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_rollback(prog: *const HDDlog) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = sync::Arc::from_raw(prog);
    let res = prog.0.lock().unwrap().transaction_rollback().map(|_|0).unwrap_or_else(|e|{
        eprintln!("ddlog_transaction_rollback(): error: {}", e);
        -1
    });
    sync::Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_apply_updates(prog: *const HDDlog, upds: *const *mut record::UpdCmd, n: size_t) -> raw::c_int
{
    if prog.is_null() || upds.is_null() {
        return -1;
    };
    let prog = sync::Arc::from_raw(prog);
    let mut upds_vec = Vec::with_capacity(n as usize);
    for i in 0..n {
        let upd = match updcmd2upd(Box::from_raw(*upds.offset(i as isize)).as_ref()) {
            Ok(upd) => upd,
            Err(e) => {
                eprintln!("ddlog_apply_updates(): invalid argument: {}", e);
                return -1;
            }
        };
        upds_vec.push(upd);
    };
    let res = prog.0.lock().unwrap().apply_updates(upds_vec).map(|_|0).unwrap_or_else(|e|{
        eprintln!("ddlog_apply_updates(): error: {}", e);
        return -1;
    });
    sync::Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_clear_relation(
    prog: *const HDDlog,
    table: libc::size_t) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = sync::Arc::from_raw(prog);
    let res = match clear_relation(&prog, table) {
        Ok(()) => { 0 },
        Err(e) => {
            eprintln!("ddlog_clear_relation(): error: {}", e);
            -1
        }
    };
    sync::Arc::into_raw(prog);
    res
}

unsafe fn clear_relation(prog: &HDDlog, table: libc::size_t) -> Result<(), String> {
    prog.0.lock().unwrap().clear_relation(table)
}

#[no_mangle]
pub extern "C" fn ddlog_dump_table(
    prog:    *const HDDlog,
    table:   libc::size_t,
    cb:      extern "C" fn(arg: *mut raw::c_void, rec: *const record::Record) -> bool,
    cb_arg:  *mut raw::c_void) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = unsafe {sync::Arc::from_raw(prog)};
    let res = if let Some(ref db) = prog.1 {
        match dump_table(&mut db.lock().unwrap(), table, cb, cb_arg) {
            Ok(recs) => {
                0
            },
            Err(e) => {
                eprintln!("ddlog_dump_table(): error: {}", e);
                -1
            }
        }
    } else {
        eprintln!("ddlog_dump_table(): cannot dump table: ddlog_run() was invoked with do_store flag set to false");
        -1
    };
    sync::Arc::into_raw(prog);
    res
}

fn dump_table(db: &mut valmap::ValMap,
              table: libc::size_t,
              cb: extern "C" fn(arg: *mut raw::c_void, rec: *const record::Record) -> bool,
              cb_arg: *mut raw::c_void) -> Result<(), String>
{
    for val in db.get_rel(table) {
        if !cb(cb_arg, &val.clone().into_record() as *const record::Record) {
            break;
        }
    };
    Ok(())
}

#[no_mangle]
pub extern "C" fn ddlog_profile(prog: *const HDDlog) -> *const raw::c_char
{
    if prog.is_null() {
        return ptr::null();
    };
    let prog = unsafe {sync::Arc::from_raw(prog)};
    let res ={
        let rprog = prog.0.lock().unwrap();
        let profile = format!("{}", rprog.profile.lock().unwrap());
        ffi::CString::new(profile).expect("Failed to convert profile string to C").into_raw()
    };
    sync::Arc::into_raw(prog);
    res
}

#[no_mangle]
pub extern "C" fn ddlog_string_free(s: *mut raw::c_char)
{
    if s.is_null() {
        return;
    };
    unsafe { ffi::CString::from_raw(s); }
}
