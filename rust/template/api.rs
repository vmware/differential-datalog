use differential_datalog::program::*;
use differential_datalog::record;
use differential_datalog::record::IntoRecord;

use std::os::raw;
use std::ptr;
use std::ffi;
use std::fs;
use std::io;
use std::os::unix;
use std::os::unix::io::{FromRawFd};
use std::mem;
use std::sync::{Mutex, Arc};
use std::iter;
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
    let table_str = unsafe{ ffi::CStr::from_ptr(tname) }.to_str().unwrap();
    match HDDlog::get_table_id(table_str) {
        Ok(relid) => relid as libc::size_t,
        Err(_) => {
            libc::size_t::max_value()
        }
    }
}

impl HDDlog {
    pub fn get_table_id(tname: &str) -> Result<Relations, String>
    {
        relname2id(tname).ok_or_else(||format!("unknown relation {}", tname))
    }
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

// TODO wrap the C API on this
impl HDDlog {
    pub fn run<F>(workers: usize,
                  do_store: bool,
                  cb: Option<F>) -> Arc<HDDlog>
    where F: Callback
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
                let cb_handler = cb.map(|f| {nhandlers+=1; CallbackUpdateHandler::new(f)});

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

        Arc::new(HDDlog{
            prog:           Mutex::new(prog),
            update_handler: handler,
            db:             Some(db),
            deltadb:        deltadb,
            print_err:      None,
            replay_file:    None})
    }
}

impl HDDlog {
    pub fn record_commands(&mut self, file: &mut Option<Mutex<fs::File>>)
    {
        mem::swap(&mut self.replay_file, file);
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_record_commands(prog: *const HDDlog, fd: unix::io::RawFd) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let mut prog = Arc::from_raw(prog);

    let file = if fd == -1 {
        None
    } else {
        Some(fs::File::from_raw_fd(fd))
    };

    let res = match Arc::get_mut(&mut prog) {
        Some(prog) => {

            let mut old_file = file.map(|f| Mutex::new(f));
            prog.record_commands(&mut old_file);
            /* Convert the old fild into FD to prevent it from closeing.
             * It is the caller's responsibility to close the file when
             * they are done with it. */
            old_file.map(|m| m.into_inner().unwrap().into_raw_fd());
            0
        },
        None => -1
    };
    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_input_snapshot(prog: *const HDDlog, fd: unix::io::RawFd) -> raw::c_int
{
    if prog.is_null() || fd < 0 {
        return -1;
    };
    let mut prog = Arc::from_raw(prog);
    let mut file = fs::File::from_raw_fd(fd);
    let res = prog.dump_input_snapshot(&mut file);
    file.into_raw_fd();
    Arc::into_raw(prog);
    if res.is_ok() { 0 } else { -1 }
}

impl HDDlog {
    pub fn dump_input_snapshot<W: io::Write>(&self, w: &mut W) -> io::Result<()>
    {
        for (rel, relname) in INPUT_RELIDMAP.iter() {
            let prog = self.prog.lock().unwrap();
            match prog.get_input_relation_data(*rel as RelId) {
                Ok(valset) => {
                    for v in valset.iter() {
                        write!(w, "insert {}[{}],", relname, v)?;
                    }
                },
                _ => match prog.get_input_relation_index(*rel as RelId) {
                    Ok(ivalset) => {
                        for v in ivalset.values() {
                            write!(w, "insert {}[{}],", relname, v)?;
                        }
                    },
                    _ => {
                        panic!("Unknown input relation {:?} in dump_input_snapshot", rel);
                    }
                }
            }
        };
        Ok(())
    }
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

impl HDDlog {
    pub fn stop(self) -> Result<(), String> {
        self.prog.into_inner().map(|p|p.stop()).unwrap()
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_start(prog: *const HDDlog) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let res = prog.transaction_start().map(|_|0).unwrap_or_else(|e|{
        prog.eprintln(&format!("ddlog_transaction_start(): error: {}", e));
        -1
    });

    Arc::into_raw(prog);
    res
}

impl HDDlog {
    pub fn transaction_start(&self) -> Result<(), String>
    {
        self.record_transaction_start();
        self.prog.lock().unwrap().transaction_start()
    }

    fn record_transaction_start(&self) {
        if let Some(ref f) = self.replay_file {
            if write!(f.lock().unwrap(), "start;\n").is_err() {
                self.eprintln("failed to record invocation in replay file");
            }
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

    let f = cb.map(|f| move |tab, rec: &record::Record, pol|
                   f(cb_arg, tab, rec as *const record::Record, pol));

    let res = prog.transaction_commit_dump_changes(f)
        .map(|_|0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_transaction_commit_dump_changes: error: {}", e));
            -1
        });

    Arc::into_raw(prog);
    res
}

impl HDDlog {
    pub fn transaction_commit_dump_changes<F>
        (&self, cb: Option<F>) -> Result<(), String>
    where F: Fn(usize, &record::Record, bool) {
        self.record_transaction_commit(true);
        *self.deltadb.lock().unwrap() = Some(DeltaMap::new());

        self.update_handler.before_commit();
        match (self.prog.lock().unwrap().transaction_commit()) {
            Ok(()) => {
                self.update_handler.after_commit(true);
                let mut delta = self.deltadb.lock().unwrap();
                HDDlog::dump_delta(delta.as_mut().unwrap(), cb);
                *delta = None;
                Ok(())
            },
            Err(e) => {
                self.update_handler.after_commit(false);
                Err(e)
            }
        }
    }

    fn dump_delta<F>(db: &mut DeltaMap,
                  cb: Option<F>)
    where F:Fn(usize, &record::Record, bool)
    {
        cb.map(|f|
               for (table_id, table_data) in db.as_ref().iter() {
                   for (val, weight) in table_data.iter() {
                       debug_assert!(*weight == 1 || *weight == -1);
                       f(*table_id as libc::size_t,
                         &val.clone().into_record(),
                         *weight == 1);
                   }
               });
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit(prog: *const HDDlog) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let res = prog.transaction_commit().map(|_|0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_transaction_commit(): error: {}", e));
        -1
    });

    Arc::into_raw(prog);
    res
}

impl HDDlog {
    pub fn transaction_commit(&self) -> Result<(), String> {
        self.record_transaction_commit(false);
        self.update_handler.before_commit();

        match (self.prog.lock().unwrap().transaction_commit()) {
            Ok(()) => {
                self.update_handler.after_commit(true);
                Ok(())
            },
            Err(e) => {
                self.update_handler.after_commit(false);
                Err(e)
            }
        }
    }

    fn record_transaction_commit(&self, record_changes: bool) {
        if let Some(ref f) = self.replay_file {
            let res = if record_changes {
                write!(f.lock().unwrap(), "commit dump_changes;\n")
            } else {
                write!(f.lock().unwrap(), "commit;\n")
            };
            if res.is_err() {
                self.eprintln("failed to record invocation in replay file");
            }
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

    let res = prog.transaction_rollback().map(|_|0).unwrap_or_else(|e|{
        prog.eprintln(&format!("ddlog_transaction_rollback(): error: {}", e));
        -1
    });
    Arc::into_raw(prog);
    res
}

impl HDDlog {
    pub fn transaction_rollback(&self) -> Result<(), String> {
        if let Err(e) = self.record_transaction_rollback() {
            return Err(e);
        }
        self.prog.lock().unwrap().transaction_rollback()
    }

    fn record_transaction_rollback(&self) -> Result<(), String> {
        if let Some(ref f) = self.replay_file {
            if write!(f.lock().unwrap(), "rollback;\n").is_err() {
                Err("failed to record invocation in replay file".to_string())
            } else {
                Ok(())
            }
        } else {
            Ok(())
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

    let mut cmds_vec: Vec<Box<record::UpdCmd>> = Vec::with_capacity(n as usize);

    for i in 0..n {
        cmds_vec.push(Box::from_raw(*upds.offset(i as isize)));
    }

    let res = prog.apply_updates(cmds_vec.iter().map(|x|x.as_ref()))
                  .map(|_|0).unwrap_or_else(|e|{
                      prog.eprintln(&format!("ddlog_apply_updates(): error: {}", e));
                      -1
                  });
    Arc::into_raw(prog);
    res
}

impl HDDlog {
    pub fn apply_updates<'a, I: iter::ExactSizeIterator<Item=&'a record::UpdCmd> + Clone>(&self, upds: I) -> Result<(), String>{
        self.record_updates(upds.clone());

        let upds_vec: Result<Vec<_>, _> = upds.map(|upd| updcmd2upd(upd))
                                              .collect();

        upds_vec.map(|upds| {
            self.prog.lock().unwrap().apply_updates(upds);
        })
    }

    pub fn record_updates<'a, I: iter::ExactSizeIterator<Item=&'a record::UpdCmd>>(&self, upds: I) {
        let n = upds.len();

        if let Some(ref f) = self.replay_file {
            let mut file = f.lock().unwrap();
            for (i, u) in upds.enumerate() {
                let sep = if i == n - 1 { ";" } else { "," };
                record_update(&mut *file, u);
                let _ = write!(file, "{}\n", sep);
            }
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

    let res = prog.clear_relation(table).map(|_|0).unwrap_or_else(|e|{
        prog.eprintln(&format!("ddlog_clear_relation(): error: {}", e));
        -1
    });
    Arc::into_raw(prog);
    res
}

impl HDDlog {
    pub fn clear_relation(&self, table: usize) -> Result<(), String> {
        self.record_clear_relation(table);
        self.prog.lock().unwrap().clear_relation(table)
    }

    fn record_clear_relation(&self, table:usize) {
        if let Some(ref f) = self.replay_file {
            if write!(f.lock().unwrap(), "clear {};\n", relid2name(table).unwrap_or(&"???")).is_err() {
                self.eprintln("failed to record invocation in replay file");
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_table(
    prog:    *const HDDlog,
    table:   libc::size_t,
    cb:      Option<extern "C" fn(arg: libc::uintptr_t, rec: *const record::Record) -> bool>,
    cb_arg:  libc::uintptr_t) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let f = cb.map(|f| move |rec: &record::Record| f(cb_arg, rec));

    let res = prog.dump_table(table, f).map(|_|0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_dump_table(): error: {}", e));
        -1
    });

    Arc::into_raw(prog);
    res
}

impl HDDlog {
    pub fn dump_table<F>(&self, table: usize, cb: Option<F>) -> Result<(), &str>
    where F: Fn(&record::Record) -> bool
    {
        self.record_dump_table(table);
        if let Some(ref db) = self.db {
            HDDlog::db_dump_table(&mut db.lock().unwrap(), table, cb);
            Ok(())
        } else {
            Err("cannot dump table: ddlog_run() was invoked with do_store flag set to false")
        }
    }

    fn record_dump_table(&self, table: usize) {
        if let Some(ref f) = self.replay_file {
            if write!(f.lock().unwrap(), "dump {};\n", relid2name(table).unwrap_or(&"???")).is_err() {
                self.eprintln("ddlog_dump_table(): failed to record invocation in replay file");
            }
        }
    }

    fn db_dump_table<F>(db: &mut ValMap,
                     table: libc::size_t,
                     cb: Option<F>)
    where F:Fn(&record::Record) -> bool
    {
        cb.map(|f|
               for val in db.get_rel(table) {
                   if !f(&val.clone().into_record()) {
                       break;
                   }
               });
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_enable_cpu_profiling(prog: *const HDDlog, enable: bool) -> raw::c_int
{
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    prog.enable_cpu_profiling(enable);

    Arc::into_raw(prog);
    0
}

impl HDDlog {
    /*
     * Controls recording of differential operator runtimes.  When enabled,
     * DDlog records each activation of every operator and prints the
     * per-operator CPU usage summary in the profile.  When disabled, the
     * recording stops, but the previously accumulated profile is preserved.
     *
     * Recording CPU events can be expensive in large dataflows and is
     * therefore disabled by default.
     */
    pub fn enable_cpu_profiling(&self, enable: bool) {
        self.record_enable_cpu_profiling(enable);
        self.prog.lock().unwrap().enable_cpu_profiling(enable);
    }

    fn record_enable_cpu_profiling(&self, enable: bool) {
        if let Some(ref f) = self.replay_file {
            if write!(f.lock().unwrap(), "profile cpu {};\n", if enable { "on" } else { "off" }).is_err() {
                self.eprintln("ddlog_cpu_profiling_enable(): failed to record invocation in replay file");
            }
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

    let res = {
        let profile = prog.profile();
        ffi::CString::new(profile).expect("Failed to convert profile string to C").into_raw()
    };
    Arc::into_raw(prog);
    res
}

impl HDDlog {
    /*
     * returns DDlog program runtime profile
     */
    pub fn profile(&self) -> String {
        self.record_profile();
        let rprog = self.prog.lock().unwrap();
        let profile: String = rprog.profile.lock().unwrap().to_string();
        profile
    }

    fn record_profile(&self) {
        if let Some(ref f) = self.replay_file {
            if write!(f.lock().unwrap(), "profile;\n").is_err() {
                self.eprintln("failed to record invocation in replay file");
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_string_free(s: *mut raw::c_char)
{
    if s.is_null() {
        return;
    };
    ffi::CString::from_raw(s);
}
