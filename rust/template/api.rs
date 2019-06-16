#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals, unused_parens, non_shorthand_field_patterns, dead_code, overflowing_literals)]

use differential_datalog::program::*;
use differential_datalog::record;
use differential_datalog::record::IntoRecord;

use std::os::raw;
use std::ptr;
use std::ffi;
use std::fs;
use std::io::Write;
use std::os::unix;
use std::os::unix::io::{IntoRawFd, FromRawFd};
use std::mem;
use std::sync::{Mutex, Arc};
use libc::size_t;

use super::valmap::*;
use super::update_handler::*;
use super::*;

pub struct HDDlog {
    pub prog: Mutex<RunningProgram<Value>>,
    pub update_handler: Box<dyn IMTUpdateHandler<Value>>,
    pub db: Option<Arc<Mutex<ValMap>>>,
    pub deltadb: Arc<Mutex<Option<DeltaMap>>>,
    pub print_err: Option<extern "C" fn(msg: *const raw::c_char)>,
    /* When set, all commands sent to the program are recorded in
     * the specified `.dat` file so that they can be replayed later. */
    pub replay_file: Option<Mutex<fs::File>>
}

impl HDDlog {
    pub fn print_err(f: Option<extern "C" fn(msg: *const raw::c_char)>, msg: &str) {
        match f {
            None    => eprintln!("{}", msg),
            Some(f) => f(ffi::CString::new(msg).unwrap().into_raw())
        }
    }

    pub fn eprintln(&self, msg: &str)
    {
        Self::print_err(self.print_err, msg)
    }
}

pub fn updcmd2upd(c: &record::UpdCmd) -> Result<Update<Value>, String>
{
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
        },
        record::UpdCmd::Modify(rident, key, rec) => {
            let relid: Relations = relident2id(rident).ok_or_else(||format!("Unknown relation {}", rident))?;
            let key = relkey_from_record(relid, key)?;
            Ok(Update::Modify{relid: relid as RelId, k: key, m: Box::new(rec.clone())})
        }
    }
}

fn relident2id(r: &record::RelIdentifier) -> Option<Relations> {
    match r {
        record::RelIdentifier::RelName(rname) => relname2id(rname),
        record::RelIdentifier::RelId(id)      => relid2rel(*id)
    }
}

fn relident2name(r: &record::RelIdentifier) -> Option<&str> {
    match r {
        record::RelIdentifier::RelName(rname) => Some(rname.as_ref()),
        record::RelIdentifier::RelId(id)      => relid2name(*id)
    }
}

fn __null_cb(_relid: RelId, _v: &Value, _w: isize) {}

#[no_mangle]
pub extern "C" fn ddlog_get_table_id(tname: *const raw::c_char) -> libc::size_t
{
    if tname.is_null() {
        return libc::size_t::max_value();
    };
    match get_table_id(tname) {
        Ok(relid) => relid as libc::size_t,
        Err(_) => {
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
pub extern "C" fn ddlog_run(
    workers: raw::c_uint,
    do_store: bool,
    cb: Option<extern "C" fn(arg: libc::uintptr_t,
                             table: libc::size_t,
                             rec: *const record::Record,
                             polarity: bool)>,
    cb_arg:  libc::uintptr_t,
    print_err: Option<extern "C" fn(msg: *const raw::c_char)>) -> *const HDDlog
{
    let workers = if workers == 0 { 1 } else { workers };

    let db: Arc<Mutex<ValMap>> = Arc::new(Mutex::new(ValMap::new()));
    let db2 = db.clone();

    let deltadb: Arc<Mutex<Option<DeltaMap>>> = Arc::new(Mutex::new(None));
    let deltadb2 = deltadb.clone();

    let handler: Box<dyn IMTUpdateHandler<Value>> =  {
        let handler_generator = move || {
            let mut nhandlers: usize = 1;

            /* Always use delta handler, which costs nothing unless it is
             * actually used*/
            let delta_handler = DeltaUpdateHandler::new(deltadb2);

            let store_handler = if do_store {
                nhandlers = nhandlers + 1;
                Some(ValMapUpdateHandler::new(db2))
            } else {
                None
            };
            let cb_handler = cb.map(|f| {nhandlers+=1; ExternCUpdateHandler::new(f, cb_arg)});

            let handler: Box<dyn UpdateHandler<Value>> = if nhandlers == 1 {
                Box::new(delta_handler)
            } else {
                let mut handlers: Vec<Box<dyn UpdateHandler<Value>>> = Vec::new();
                handlers.push(Box::new(delta_handler));
                store_handler.map(|h| handlers.push(Box::new(h)));
                cb_handler.map(|h| handlers.push(Box::new(h)));
                Box::new(ChainedUpdateHandler::new(handlers))
            };
            handler
        };
        Box::new(ThreadUpdateHandler::new(handler_generator))
    };

    let program = prog(handler.mt_update_cb());

    /* Notify handler about initial transaction */
    handler.before_commit();
    let prog = program.run(workers as usize);
    handler.after_commit(true);

    Arc::into_raw(Arc::new(HDDlog{
        prog:           Mutex::new(prog),
        update_handler: handler,
        db:             Some(db),
        deltadb:        deltadb,
        print_err:      print_err,
        replay_file:    None}))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_record_commands(prog: *const HDDlog, fd: unix::io::RawFd) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let mut prog = Arc::from_raw(prog);
    let res = match Arc::get_mut(&mut prog) {
        Some(prog) => {
            /* Swap out the old file and convert it into FD to prevent
             * the file from closing on destruction (it is the caller's
             * responsibility to close the file when they're done with it). */
            let mut old_file = if fd == -1 {
                None
            } else {
                Some(Mutex::new(fs::File::from_raw_fd(fd)))
            };
            mem::swap(&mut prog.replay_file, &mut old_file);
            match old_file {
                None => (),
                Some(m) => {
                    m.into_inner().unwrap().into_raw_fd();
                }
            };
            0
        },
        None => -1
    };
    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_stop(prog: *const HDDlog) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    /* Prevents closing of the old descriptor. */
    ddlog_record_commands(prog, -1);

    let prog = Arc::from_raw(prog);
    match Arc::try_unwrap(prog) {
        Ok(HDDlog{prog, print_err, ..}) => {
            prog.into_inner()
                .map(|p|p.stop().map(|_|0).unwrap_or_else(|e| {
                    HDDlog::print_err(print_err, &format!("ddlog_stop(): error: {}", e));
                    -1
                }))
                .unwrap_or_else(|e|{
                    HDDlog::print_err(print_err, &format!("ddlog_stop(): error acquiring lock: {}", e));
                    -1
                })
        },
        Err(pref) => {
            pref.eprintln("ddlog_stop(): cannot extract value from Arc");
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
    let prog = Arc::from_raw(prog);

    record_transaction_start(&prog);

    let res = prog.prog.lock().unwrap().transaction_start().map(|_|0).unwrap_or_else(|e|{
        prog.eprintln(&format!("ddlog_transaction_start(): error: {}", e));
        -1
    });
    Arc::into_raw(prog);
    res
}

unsafe fn record_transaction_start(prog: &Arc<HDDlog>) {
    if let Some(ref f) = prog.replay_file {
        if write!(f.lock().unwrap(), "start;\n").is_err() {
            prog.eprintln("ddlog_transaction_start(): failed to record invocation in replay file");
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit_dump_changes(
    prog: *const HDDlog,
    cb: Option<extern "C" fn(arg: libc::uintptr_t,
                             table: libc::size_t,
                             rec: *const record::Record,
                             polarity: bool)>,
    cb_arg:  libc::uintptr_t) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    record_transaction_commit(&prog, true);
    *prog.deltadb.lock().unwrap() = Some(DeltaMap::new());

    prog.update_handler.before_commit();

    let res = match (prog.prog.lock().unwrap().transaction_commit()) {
        Ok(()) => {
            prog.update_handler.after_commit(true);
            let mut delta = prog.deltadb.lock().unwrap();
            dump_delta(delta.as_mut().unwrap(), cb, cb_arg);
            *delta = None;
            0
        },
        Err(e) => {
            prog.update_handler.after_commit(false);
            prog.eprintln(&format!("ddlog_transaction_commit_dump_changes(): error: {}", e));
            -1
        }
    };

    Arc::into_raw(prog);
    res
}

fn dump_delta(db: &mut DeltaMap,
              cb: Option<extern "C" fn(arg: libc::uintptr_t,
                                       table: libc::size_t,
                                       rec: *const record::Record,
                                       polarity: bool)>,
              cb_arg: libc::uintptr_t)
{
    cb.map(|f|
           for (table_id, table_data) in db.as_ref().iter() {
               for (val, weight) in table_data.iter() {
                   debug_assert!(*weight == 1 || *weight == -1);
                   f(cb_arg,
                     *table_id as libc::size_t,
                     &val.clone().into_record() as *const record::Record,
                     *weight == 1);
               }
           });
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit(prog: *const HDDlog) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    record_transaction_commit(&prog, false);
    prog.update_handler.before_commit();

    let res = match (prog.prog.lock().unwrap().transaction_commit()) {
        Ok(()) => {
            prog.update_handler.after_commit(true);
            0
        },
        Err(e) => {
            prog.update_handler.after_commit(false);
            prog.eprintln(&format!("ddlog_transaction_commit(): error: {}", e));
            -1
        }
    };
    Arc::into_raw(prog);
    res
}

unsafe fn record_transaction_commit(prog: &Arc<HDDlog>, record_changes: bool) {
    if let Some(ref f) = prog.replay_file {
        let res = if record_changes {
            write!(f.lock().unwrap(), "commit dump_changes;\n")
        } else {
            write!(f.lock().unwrap(), "commit;\n")
        };
        if res.is_err() {
            prog.eprintln("ddlog_transaction_commit(): failed to record invocation in replay file");
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_rollback(prog: *const HDDlog) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    record_transaction_rollback(&prog);

    let res = prog.prog.lock().unwrap().transaction_rollback().map(|_|0).unwrap_or_else(|e|{
        prog.eprintln(&format!("ddlog_transaction_rollback(): error: {}", e));
        -1
    });
    Arc::into_raw(prog);
    res
}

unsafe fn record_transaction_rollback(prog: &Arc<HDDlog>) {
    if let Some(ref f) = prog.replay_file {
        if write!(f.lock().unwrap(), "rollback;\n").is_err() {
            prog.eprintln("ddlog_transaction_rollback(): failed to record invocation in replay file");
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_apply_updates(prog: *const HDDlog, upds: *const *mut record::UpdCmd, n: size_t) -> raw::c_int
{
    if prog.is_null() || upds.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    record_updates(&prog, upds, n);

    let mut upds_vec = Vec::with_capacity(n as usize);
    for i in 0..n {
        let upd = match updcmd2upd(Box::from_raw(*upds.offset(i as isize)).as_ref()) {
            Ok(upd) => upd,
            Err(e) => {
                prog.eprintln(&format!("ddlog_apply_updates(): invalid argument: {}", e));
                return -1;
            }
        };
        upds_vec.push(upd);
    };
    let res = prog.prog.lock().unwrap().apply_updates(upds_vec).map(|_|0).unwrap_or_else(|e|{
        prog.eprintln(&format!("ddlog_apply_updates(): error: {}", e));
        return -1;
    });
    Arc::into_raw(prog);
    res
}

unsafe fn record_updates(prog: &Arc<HDDlog>, upds: *const *mut record::UpdCmd, n: size_t)
{
    if let Some(ref f) = prog.replay_file {
        let mut file = f.lock().unwrap();
        for i in 0..n {
            let sep = if i == n - 1 { ";" } else { "," };
            record_update(&mut *file, &(**upds.offset(i as isize)));
            let _ = write!(file, "{}\n", sep);
        }
    }
}

pub fn record_update(file: &mut fs::File, upd: &record::UpdCmd)
{
    match upd {
        record::UpdCmd::Insert(rel, record) => {
            let _ = write!(file, "insert {}[{}]", relident2name(rel).unwrap_or(&"???"), record);
        },
        record::UpdCmd::Delete(rel, record) => {
            let _ = write!(file, "delete {}[{}]", relident2name(rel).unwrap_or(&"???"), record);
        },
        record::UpdCmd::DeleteKey(rel, record) => {
            let _ = write!(file, "delete_key {} {}", relident2name(rel).unwrap_or(&"???"), record);
        },
        record::UpdCmd::Modify(rel, key, mutator) => {
            let _ = write!(file, "modify {} {} <- {}", relident2name(rel).unwrap_or(&"???"), key, mutator);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_clear_relation(
    prog: *const HDDlog,
    table: libc::size_t) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    record_clear_relation(&prog, table);

    let res = match clear_relation(&prog, table) {
        Ok(()) => { 0 },
        Err(e) => {
            prog.eprintln(&format!("ddlog_clear_relation(): error: {}", e));
            -1
        }
    };
    Arc::into_raw(prog);
    res
}

unsafe fn record_clear_relation(prog: &Arc<HDDlog>, table: libc::size_t) {
    if let Some(ref f) = prog.replay_file {
        if write!(f.lock().unwrap(), "clear {};\n", relid2name(table).unwrap_or(&"???")).is_err() {
            prog.eprintln("ddlog_clear_relation(): failed to record invocation in replay file");
        }
    }
}

unsafe fn clear_relation(prog: &HDDlog, table: libc::size_t) -> Result<(), String> {
    prog.prog.lock().unwrap().clear_relation(table)
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_table(
    prog:    *const HDDlog,
    table:   libc::size_t,
    cb:      Option<extern "C" fn(arg: *mut raw::c_void, rec: *const record::Record) -> bool>,
    cb_arg:  *mut raw::c_void) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    record_dump_table(&prog, table);

    let res = if let Some(ref db) = prog.db {
        dump_table(&mut db.lock().unwrap(), table, cb, cb_arg);
        0
    } else {
        prog.eprintln("ddlog_dump_table(): cannot dump table: ddlog_run() was invoked with do_store flag set to false");
        -1
    };
    Arc::into_raw(prog);
    res
}

unsafe fn record_dump_table(prog: &Arc<HDDlog>, table: libc::size_t) {
    if let Some(ref f) = prog.replay_file {
        if write!(f.lock().unwrap(), "dump {};\n", relid2name(table).unwrap_or(&"???")).is_err() {
            prog.eprintln("ddlog_dump_table(): failed to record invocation in replay file");
        }
    }
}

fn dump_table(db: &mut ValMap,
              table: libc::size_t,
              cb: Option<extern "C" fn(arg: *mut raw::c_void, rec: *const record::Record) -> bool>,
              cb_arg: *mut raw::c_void)
{
    cb.map(|f|
           for val in db.get_rel(table) {
               if !f(cb_arg, &val.clone().into_record() as *const record::Record) {
                   break;
               }
           });
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_enable_cpu_profiling(prog: *const HDDlog, enable: bool) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    record_enable_cpu_profiling(&prog, enable);

    prog.prog.lock().unwrap().enable_cpu_profiling(enable);
    Arc::into_raw(prog);
    0
}

fn record_enable_cpu_profiling(prog: &Arc<HDDlog>, enable: bool) {
    if let Some(ref f) = prog.replay_file {
        if write!(f.lock().unwrap(), "profile cpu {};\n", if enable { "on" } else { "off" }).is_err() {
            prog.eprintln("ddlog_cpu_profiling_enable(): failed to record invocation in replay file");
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_profile(prog: *const HDDlog) -> *const raw::c_char
{
    if prog.is_null() {
        return ptr::null();
    };
    let prog = Arc::from_raw(prog);

    record_profile(&prog);

    let res = {
        let rprog = prog.prog.lock().unwrap();
        let profile = format!("{}", rprog.profile.lock().unwrap());
        ffi::CString::new(profile).expect("Failed to convert profile string to C").into_raw()
    };
    Arc::into_raw(prog);
    res
}

unsafe fn record_profile(prog: &Arc<HDDlog>) {
    if let Some(ref f) = prog.replay_file {
        if write!(f.lock().unwrap(), "profile;\n").is_err() {
            prog.eprintln("ddlog_profile(): failed to record invocation in replay file");
        }
    }
}

#[no_mangle]
pub extern "C" fn ddlog_string_free(s: *mut raw::c_char)
{
    if s.is_null() {
        return;
    };
    unsafe { ffi::CString::from_raw(s); }
}
