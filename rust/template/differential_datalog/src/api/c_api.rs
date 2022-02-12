//! C Bindings
#![cfg(feature = "c_api")]
#![allow(non_camel_case_types)]

use crate::{
    api::{update_handler::ExternCCallback, HDDlog},
    ddval::DDValue,
    program::{
        config::{Config, LoggingDestination, ProfilingConfig},
        IdxId, RelId,
    },
    record::{IntoRecord, Record, UpdCmd},
    DDlog, DDlogDump, DDlogDynamic, DDlogInventory, DDlogProfiling, DeltaMap,
};
use std::{
    collections::BTreeMap,
    ffi::{CStr, CString},
    fs::File,
    mem::ManuallyDrop,
    net::SocketAddr,
    os::raw,
    ptr,
    ptr::NonNull,
    slice,
    str::FromStr,
};
use triomphe::Arc;

#[cfg(unix)]
use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{FromRawHandle, IntoRawHandle, RawHandle};

// FIXME: Unwinding across the FFI boundary is UB, catch all panics and return errors

/// IMPORTANT: The followint typedefs must be kept in sync with equivalent declarations in ddlog.h.

#[repr(C)]
pub enum ddlog_profiling_mode {
    ddlog_disable_profiling = 0,
    ddlog_self_profiling = 1,
    ddlog_timely_profiling = 2,
}

#[repr(C)]
pub enum ddlog_log_mode {
    ddlog_log_disabled = 0,
    ddlog_log_to_socket = 1,
    ddlog_log_to_disk = 2,
}

#[repr(C)]
pub struct ddlog_log_destination {
    pub mode: ddlog_log_mode,
    pub address_str: *const raw::c_char,
}

impl Default for ddlog_log_destination {
    fn default() -> Self {
        ddlog_log_destination {
            mode: ddlog_log_mode::ddlog_log_disabled,
            address_str: ptr::null_mut(),
        }
    }
}

impl ddlog_log_destination {
    pub unsafe fn to_rust_api(&self) -> Result<Option<LoggingDestination>, String> {
        let address = if self.address_str.is_null() {
            None
        } else {
            Some(
                CStr::from_ptr(self.address_str)
                    .to_str()
                    .map_err(|e| format!("invalid address string: {}", e))?
                    .to_string(),
            )
        };
        Ok(match self.mode {
            ddlog_log_mode::ddlog_log_disabled => None,
            ddlog_log_mode::ddlog_log_to_socket => Some(LoggingDestination::Socket {
                sockaddr: SocketAddr::from_str(address.as_ref().ok_or_else(|| {
                    "ddlog_log_to_socket requires socket address in address_str".to_string()
                })?)
                .map_err(|e| {
                    format!("invalid socket address string {}: {}", address.unwrap(), e)
                })?,
            }),
            ddlog_log_mode::ddlog_log_to_disk => Some(LoggingDestination::Disk {
                directory: address.ok_or_else(|| {
                    "ddlog_log_to_disk requires target directory name in addres_str".to_string()
                })?,
            }),
        })
    }
}

#[repr(C)]
pub struct ddlog_profiling_config {
    pub mode: ddlog_profiling_mode,
    pub self_profiler_dir: *const raw::c_char,
    pub timely_destination: ddlog_log_destination,
    pub timely_progress_destination: ddlog_log_destination,
    pub differential_destination: ddlog_log_destination,
}

impl ddlog_profiling_config {
    pub unsafe fn to_rust_api(&self) -> Result<ProfilingConfig, String> {
        Ok(match self.mode {
            ddlog_profiling_mode::ddlog_disable_profiling => ProfilingConfig::None,
            ddlog_profiling_mode::ddlog_self_profiling => {
                ProfilingConfig::SelfProfiling {
                    profile_directory: {
                        if self.self_profiler_dir.is_null() {
                            None
                        } else {
                            Some(CStr::from_ptr(self.self_profiler_dir)
                                .to_str()
                                .map_err(|e| format!("invalid profile directory string: {}", e))?
                                .to_string())
                        }
                    }
                }
            },
            ddlog_profiling_mode::ddlog_timely_profiling => {
                ProfilingConfig::TimelyProfiling {
                    timely_destination: self.timely_destination.to_rust_api()?
                        .ok_or_else(|| "ddlog_timely_profiling requires socket address or directory path in timely_destination".to_string())?,
                    timely_progress_destination: self.timely_progress_destination.to_rust_api()?,
                    differential_destination: self.differential_destination.to_rust_api()?
                }
            }
        })
    }
}

impl Default for ddlog_profiling_config {
    fn default() -> Self {
        ddlog_profiling_config {
            mode: ddlog_profiling_mode::ddlog_disable_profiling,
            self_profiler_dir: ptr::null(),
            timely_destination: ddlog_log_destination::default(),
            timely_progress_destination: ddlog_log_destination::default(),
            differential_destination: ddlog_log_destination::default(),
        }
    }
}

#[repr(C)]
pub struct ddlog_config {
    pub num_timely_workers: raw::c_uint,
    pub enable_debug_regions: bool,
    pub profiling_config: ddlog_profiling_config,
    pub differential_idle_merge_effort: libc::ssize_t,
}

impl Default for ddlog_config {
    fn default() -> Self {
        let default_rust_cfg = Config::default();
        ddlog_config {
            num_timely_workers: default_rust_cfg.num_timely_workers as raw::c_uint,
            enable_debug_regions: default_rust_cfg.enable_debug_regions,
            profiling_config: ddlog_profiling_config::default(),
            differential_idle_merge_effort: default_rust_cfg
                .differential_idle_merge_effort
                .unwrap_or(0),
        }
    }
}

impl ddlog_config {
    pub unsafe fn to_rust_api(&self) -> Result<Config, String> {
        Ok(Config {
            num_timely_workers: self.num_timely_workers as usize,
            enable_debug_regions: self.enable_debug_regions,
            profiling_config: self.profiling_config.to_rust_api()?,
            differential_idle_merge_effort: if self.differential_idle_merge_effort == 0 {
                None
            } else {
                Some(self.differential_idle_merge_effort)
            },
        })
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_table_id(
    prog: *const HDDlog,
    tname: *const raw::c_char,
) -> libc::size_t {
    if prog.is_null() || tname.is_null() {
        return libc::size_t::max_value();
    }

    let prog = &*prog;

    let table_str = CStr::from_ptr(tname).to_str().unwrap();
    match prog.inventory.get_table_id(table_str) {
        Ok(relid) => relid as libc::size_t,
        Err(_) => libc::size_t::max_value(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_table_original_name(
    prog: *const HDDlog,
    tname: *const raw::c_char,
) -> *const raw::c_char {
    if prog.is_null() || tname.is_null() {
        return ptr::null();
    }

    let prog = &*prog;

    let table_str = CStr::from_ptr(tname).to_str().unwrap();
    match prog.inventory.get_table_original_cname(table_str) {
        Ok(name) => name.as_ptr(),
        Err(_) => ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_table_name(
    prog: *const HDDlog,
    tid: libc::size_t,
) -> *const raw::c_char {
    if prog.is_null() {
        return ptr::null();
    }

    let prog = &*prog;

    match prog.inventory.get_table_cname(tid) {
        Ok(name) => name.as_ptr(),
        Err(_) => ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_index_id(
    prog: *const HDDlog,
    iname: *const raw::c_char,
) -> libc::size_t {
    if prog.is_null() {
        return libc::size_t::max_value();
    }

    let prog = &*prog;

    if iname.is_null() {
        return libc::size_t::max_value();
    }

    let index_str = CStr::from_ptr(iname).to_str().unwrap();
    match prog.inventory.get_index_id(index_str) {
        Ok(idxid) => idxid as libc::size_t,
        Err(_) => libc::size_t::max_value(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_index_name(
    prog: *const HDDlog,
    iid: libc::size_t,
) -> *const raw::c_char {
    if prog.is_null() {
        return ptr::null();
    }

    let prog = &*prog;

    match prog.inventory.get_index_cname(iid) {
        Ok(name) => name.as_ptr(),
        Err(_) => ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn ddlog_default_config() -> ddlog_config {
    Default::default()
}

#[no_mangle]
#[cfg(unix)]
pub unsafe extern "C" fn ddlog_record_commands(prog: *const HDDlog, fd: RawFd) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let mut prog = ManuallyDrop::new(Arc::from_raw(prog));

    let mut file = if fd == -1 {
        None
    } else {
        Some(File::from_raw_fd(fd))
    };

    match Arc::get_mut(&mut prog) {
        Some(prog) => {
            prog.record_commands(&mut file);
            /* Convert the old file into FD to prevent it from closing.
             * It is the caller's responsibility to close the file when
             * they are done with it. */
            file.map(|m| m.into_raw_fd());
            0
        }
        None => -1,
    }
}

#[no_mangle]
#[cfg(windows)]
pub unsafe extern "C" fn ddlog_record_commands(prog: *const HDDlog, fd: raw::c_int) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let mut prog = ManuallyDrop::new(Arc::from_raw(prog));

    let mut file = if fd == -1 {
        None
    } else {
        // Convert file descriptor to file handle on Windows.
        let handle = libc::get_osfhandle(fd);
        Some(File::from_raw_handle(handle as RawHandle))
    };

    match Arc::get_mut(&mut prog) {
        Some(prog) => {
            prog.record_commands(&mut file);
            /* Convert the old file into FD to prevent it from closing.
             * It is the caller's responsibility to close the file when
             * they are done with it. */
            file.map(|m| m.into_raw_handle());
            0
        }
        None => -1,
    }
}

#[no_mangle]
#[cfg(unix)]
pub unsafe extern "C" fn ddlog_dump_input_snapshot(prog: *const HDDlog, fd: RawFd) -> raw::c_int {
    if prog.is_null() || fd < 0 {
        return -1;
    }

    let prog = &*prog;
    let mut file = File::from_raw_fd(fd);
    let res = prog
        .dump_input_snapshot(&mut file)
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_dump_input_snapshot: error: {}", e));
            -1
        });

    file.into_raw_fd();
    res
}

#[no_mangle]
#[cfg(windows)]
pub unsafe extern "C" fn ddlog_dump_input_snapshot(
    prog: *const HDDlog,
    fd: raw::c_int,
) -> raw::c_int {
    if prog.is_null() || fd < 0 {
        return -1;
    }

    let prog = &*prog;
    // Convert file descriptor to file handle on Windows.
    let handle = libc::get_osfhandle(fd);
    let mut file = File::from_raw_handle(handle as RawHandle);
    let res = prog
        .dump_input_snapshot(&mut file)
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_dump_input_snapshot: error: {}", e));
            -1
        });

    file.into_raw_handle();
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_stop(prog: *const HDDlog) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    /* Prevents closing of the old descriptor. */
    ddlog_record_commands(prog, -1);

    let prog = Arc::from_raw(prog);

    let &HDDlog {
        ref prog,
        print_err,
        ..
    } = &*prog;

    prog.lock()
        .map(|mut program| {
            program.stop().map(|_| 0).unwrap_or_else(|e| {
                HDDlog::print_err(print_err, &format!("ddlog_stop(): error: {}", e));
                -1
            })
        })
        .unwrap_or_else(|e| {
            HDDlog::print_err(
                print_err,
                &format!("ddlog_stop(): error acquiring lock: {}", e),
            );
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_start(prog: *const HDDlog) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.transaction_start().map(|_| 0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_transaction_start(): error: {}", e));
        -1
    })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit_dump_changes(
    prog: *const HDDlog,
) -> *mut DeltaMap<DDValue> {
    if prog.is_null() {
        return ptr::null_mut();
    }
    let prog = &*prog;

    prog.transaction_commit_dump_changes()
        .map(|delta| Box::into_raw(Box::new(delta)))
        .unwrap_or_else(|e| {
            prog.eprintln(&format!(
                "ddlog_transaction_commit_dump_changes: error: {}",
                e
            ));
            ptr::null_mut()
        })
}

#[repr(C)]
pub struct ddlog_record_update {
    table: libc::size_t,
    rec: *mut Record,
    w: libc::ssize_t,
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit_dump_changes_as_array(
    prog: *const HDDlog,
    changes: *mut *const ddlog_record_update,
    num_changes: *mut libc::size_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    do_transaction_commit_dump_changes_as_array(prog, changes, num_changes)
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!(
                "ddlog_transaction_commit_dump_changes_as_array: error: {}",
                e
            ));
            -1
        })
}

unsafe fn do_transaction_commit_dump_changes_as_array(
    prog: &HDDlog,
    changes: *mut *const ddlog_record_update,
    num_changes: *mut libc::size_t,
) -> Result<(), String> {
    let updates = prog.transaction_commit_dump_changes()?;
    let mut size = 0;
    for (_, delta) in updates.as_ref().iter() {
        size += delta.len();
    }

    *num_changes = size;
    // Make sure that vector's capacity will be equal to its length.
    let mut change_vec = Vec::with_capacity(size);
    for (rel, delta) in updates.into_iter() {
        for (val, w) in delta.into_iter() {
            change_vec.push(ddlog_record_update {
                table: rel,
                rec: Box::into_raw(Box::new(val.into_record())),
                w,
            });
        }
    }

    *changes = change_vec.as_ptr();
    std::mem::forget(change_vec);

    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_free_record_updates(
    changes: *mut ddlog_record_update,
    num_changes: libc::size_t,
) {
    // Assume that vector's capacity is equal to its length.
    let changes_vec: Vec<ddlog_record_update> =
        Vec::from_raw_parts(changes, num_changes as usize, num_changes as usize);
    for upd in changes_vec.into_iter() {
        let _update = Box::from_raw(upd.rec);
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit_dump_changes_to_flatbuf(
    prog: *const HDDlog,
    buf: *mut *const u8,
    buf_size: *mut libc::size_t,
    buf_capacity: *mut libc::size_t,
    buf_offset: *mut libc::size_t,
) -> raw::c_int {
    if prog.is_null() || buf_size.is_null() || buf_capacity.is_null() || buf_offset.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.transaction_commit_dump_changes()
        .and_then(|changes| {
            let (flatbuf_vec, flatbuf_offset) =
                prog.flatbuf_converter.updates_to_buffer(&changes)?;
            let flatbuf_vec = ManuallyDrop::new(flatbuf_vec);

            *buf = flatbuf_vec.as_ptr();
            *buf_size = flatbuf_vec.len() as libc::size_t;
            *buf_capacity = flatbuf_vec.capacity() as libc::size_t;
            *buf_offset = flatbuf_offset as libc::size_t;

            Ok(0)
        })
        .unwrap_or_else(|e| {
            prog.eprintln(&format!(
                "ddlog_transaction_commit_dump_changes_to_flatbuf: error: {}",
                e
            ));
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_query_index_from_flatbuf(
    prog: *const HDDlog,
    buf: *const u8,
    n: libc::size_t,
    resbuf: *mut *const u8,
    resbuf_size: *mut libc::size_t,
    resbuf_capacity: *mut libc::size_t,
    resbuf_offset: *mut libc::size_t,
) -> raw::c_int {
    if prog.is_null()
        || buf.is_null()
        || resbuf.is_null()
        || resbuf_size.is_null()
        || resbuf_capacity.is_null()
        || resbuf_offset.is_null()
    {
        return -1;
    }

    let prog = &*prog;

    prog.flatbuf_converter
        .query_index_from_buffer(slice::from_raw_parts(buf, n))
        .and_then(|(index_id, key)| {
            prog.query_index(index_id, key)
                .map(|contents| (index_id, contents))
        })
        .and_then(|(index_id, contents)| {
            let (flatbuf_vec, flatbuf_offset) = prog
                .flatbuf_converter
                .index_values_to_buffer(index_id, &contents)?;
            let flatbuf_vec = ManuallyDrop::new(flatbuf_vec);

            *resbuf = flatbuf_vec.as_ptr();
            *resbuf_size = flatbuf_vec.len() as libc::size_t;
            *resbuf_capacity = flatbuf_vec.capacity() as libc::size_t;
            *resbuf_offset = flatbuf_offset as libc::size_t;

            Ok(0)
        })
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_query_index_from_flatbuf(): error: {}", e));
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_index(
    prog: *const HDDlog,
    idxid: libc::size_t,
    cb: Option<extern "C" fn(arg: libc::uintptr_t, rec: *const Record)>,
    cb_arg: libc::uintptr_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.dump_index(idxid as IdxId)
        .map(|set| {
            if let Some(f) = cb {
                for val in set.iter() {
                    f(cb_arg, &val.clone().into_record());
                }
            };
            0
        })
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_dump_index: error: {}", e));
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_query_index(
    prog: *const HDDlog,
    idxid: libc::size_t,
    key: *const Record,
    cb: Option<extern "C" fn(arg: libc::uintptr_t, rec: *const Record)>,
    cb_arg: libc::uintptr_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.query_index_dynamic(idxid as IdxId, &*key)
        .map(|set| {
            if let Some(f) = cb {
                for val in set.iter() {
                    f(cb_arg, val);
                }
            }
            0
        })
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_dump_index: error: {}", e));
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_index_to_flatbuf(
    prog: *const HDDlog,
    idxid: libc::size_t,
    resbuf: *mut *const u8,
    resbuf_size: *mut libc::size_t,
    resbuf_capacity: *mut libc::size_t,
    resbuf_offset: *mut libc::size_t,
) -> raw::c_int {
    if prog.is_null()
        || resbuf.is_null()
        || resbuf_size.is_null()
        || resbuf_capacity.is_null()
        || resbuf_offset.is_null()
    {
        return -1;
    }
    let prog = &*prog;

    prog.dump_index(idxid as IdxId)
        .and_then(|contents| {
            let (flatbuf_vec, flatbuf_offset) = prog
                .flatbuf_converter
                .index_values_to_buffer(idxid, &contents)?;
            let flatbuf_vec = ManuallyDrop::new(flatbuf_vec);

            *resbuf = flatbuf_vec.as_ptr();
            *resbuf_size = flatbuf_vec.len() as libc::size_t;
            *resbuf_capacity = flatbuf_vec.capacity() as libc::size_t;
            *resbuf_offset = flatbuf_offset as libc::size_t;

            Ok(0)
        })
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_dump_index_to_flatbuf(): error: {}", e));
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_flatbuf_free(
    buf: *mut u8,
    buf_size: libc::size_t,
    buf_capacity: libc::size_t,
) {
    Vec::from_raw_parts(buf, buf_size as usize, buf_capacity as usize);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit(prog: *const HDDlog) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.transaction_commit().map(|_| 0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_transaction_commit(): error: {}", e));
        -1
    })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_rollback(prog: *const HDDlog) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.transaction_rollback().map(|_| 0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_transaction_rollback(): error: {}", e));
        -1
    })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_apply_updates(
    prog: *const HDDlog,
    upds: *const *mut UpdCmd,
    n: libc::size_t,
) -> raw::c_int {
    if prog.is_null() || upds.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.apply_updates_dynamic(&mut (0..n).map(|i| *Box::from_raw(*upds.add(i))))
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_apply_updates(): error: {}", e));
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_apply_updates_from_flatbuf(
    prog: *const HDDlog,
    buf: *const u8,
    n: libc::size_t,
) -> raw::c_int {
    if prog.is_null() || buf.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.apply_updates_from_flatbuf(slice::from_raw_parts(buf, n))
        .map_or_else(
            |e| {
                prog.eprintln(&format!("ddlog_apply_updates_from_flatbuf(): error: {}", e));
                -1
            },
            |_| 0,
        )
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_clear_relation(
    prog: *const HDDlog,
    table: libc::size_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.clear_relation(table).map(|_| 0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_clear_relation(): error: {}", e));
        -1
    })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_table(
    prog: *const HDDlog,
    table: libc::size_t,
    cb: Option<extern "C" fn(arg: libc::uintptr_t, rec: *const Record, w: libc::ssize_t) -> bool>,
    cb_arg: libc::uintptr_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    match cb {
        None => prog.dump_table(table, None).map(|_| 0).unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_dump_table(): error: {}", e));
            -1
        }),
        Some(f) => {
            let f = move |rec: &Record, w: isize| f(cb_arg, rec, w as libc::ssize_t);
            prog.dump_table(table, Some(&f))
                .map(|_| 0)
                .unwrap_or_else(|e| {
                    prog.eprintln(&format!("ddlog_dump_table(): error: {}", e));
                    -1
                })
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_enable_cpu_profiling(
    prog: *const HDDlog,
    enable: bool,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.enable_cpu_profiling(enable)
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_enable_cpu_profiling(): error: {}", e));
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_enable_change_profiling(
    prog: *const HDDlog,
    enable: bool,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.enable_change_profiling(enable)
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_enable_change_profiling(): error: {}", e));
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_enable_timely_profiling(
    prog: *const HDDlog,
    enable: bool,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    }
    let prog = &*prog;

    prog.enable_timely_profiling(enable)
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_enable_timely_profiling(): error: {}", e));
            -1
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_profile(
    prog: Option<NonNull<HDDlog>>,
    label: Option<NonNull<raw::c_char>>,
) -> Option<NonNull<raw::c_char>> {
    let prog = prog?;
    let prog = prog.as_ref();
    let label = match label {
        None => None,
        Some(label) => Some(
            CStr::from_ptr(label.as_ptr())
                .to_str()
                .map_err(|err| prog.eprintln(&format!("invalid label string: {}", err)))
                .ok()?,
        ),
    };
    let prof_file = prog
        .dump_profile(label)
        .map_err(|err| prog.eprintln(&format!("failed to dump profile: {}", err)))
        .ok()?;
    match CString::new(prof_file).map(CString::into_raw) {
        Ok(string) => NonNull::new(string),
        Err(err) => {
            prog.eprintln(&format!(
                "failed to convert profile filename to a C string: {}",
                err,
            ));
            None
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_arrangement_size_profile(prog: *const HDDlog) -> *const raw::c_char {
    if prog.is_null() {
        return ptr::null();
    }
    let prog = &*prog;

    let profile = match prog.arrangement_size_profile() {
        Err(e) => {
            prog.eprintln(&format!(
                "Failed to retrieve arrangement size profile: {}",
                e
            ));
            return ptr::null_mut();
        }
        Ok(profile) => profile,
    };
    let profile_json = match serde_json::to_string(&profile) {
        Err(e) => {
            prog.eprintln(&format!(
                "Failed to convert arrangement size profile to JSON: {}",
                e
            ));
            return ptr::null_mut();
        }
        Ok(json) => json,
    };
    CString::new(profile_json)
        .map(CString::into_raw)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("Failed to convert profile to C string: {}", e));
            ptr::null_mut()
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_peak_arrangement_size_profile(
    prog: *const HDDlog,
) -> *const raw::c_char {
    if prog.is_null() {
        return ptr::null();
    }
    let prog = &*prog;

    let profile = match prog.peak_arrangement_size_profile() {
        Err(e) => {
            prog.eprintln(&format!(
                "Failed to retrieve peak arrangement size profile: {}",
                e
            ));
            return ptr::null_mut();
        }
        Ok(profile) => profile,
    };
    let profile_json = match serde_json::to_string(&profile) {
        Err(e) => {
            prog.eprintln(&format!(
                "Failed to convert peak arrangement size profile to JSON: {}",
                e
            ));
            return ptr::null_mut();
        }
        Ok(json) => json,
    };
    CString::new(profile_json)
        .map(CString::into_raw)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("Failed to convert profile to C string: {}", e));
            ptr::null_mut()
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_change_profile(prog: *const HDDlog) -> *const raw::c_char {
    if prog.is_null() {
        return ptr::null();
    }
    let prog = &*prog;

    let profile = match prog.change_profile() {
        Err(e) => {
            prog.eprintln(&format!("Failed to retrieve change profile: {}", e));
            return ptr::null_mut();
        }
        Ok(None) => vec![],
        Ok(Some(records)) => records,
    };
    let profile_json = match serde_json::to_string(&profile) {
        Err(e) => {
            prog.eprintln(&format!("Failed to convert change profile to JSON: {}", e));
            return ptr::null_mut();
        }
        Ok(json) => json,
    };
    CString::new(profile_json)
        .map(CString::into_raw)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("Failed to convert profile to C string: {}", e));
            ptr::null_mut()
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_cpu_profile(prog: *const HDDlog) -> *const raw::c_char {
    if prog.is_null() {
        return ptr::null();
    }
    let prog = &*prog;

    let profile = match prog.cpu_profile() {
        Err(e) => {
            prog.eprintln(&format!("Failed to retrieve CPU profile: {}", e));
            return ptr::null_mut();
        }
        Ok(None) => vec![],
        Ok(Some(profile)) => profile,
    };
    let profile_json = match serde_json::to_string(&profile) {
        Err(e) => {
            prog.eprintln(&format!("Failed to convert CPU profile to JSON: {}", e));
            return ptr::null_mut();
        }
        Ok(json) => json,
    };
    CString::new(profile_json)
        .map(CString::into_raw)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("Failed to convert profile to C string: {}", e));
            ptr::null_mut()
        })
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_string_free(s: *mut raw::c_char) {
    if s.is_null() {
        return;
    }

    drop(CString::from_raw(s));
}

#[no_mangle]
pub extern "C" fn ddlog_new_delta() -> *mut DeltaMap<DDValue> {
    Box::into_raw(Box::new(DeltaMap::new()))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delta_get_table(
    delta: *const DeltaMap<DDValue>,
    table: libc::size_t,
) -> *mut DeltaMap<DDValue> {
    let res = DeltaMap::singleton(
        table,
        (&*delta)
            .try_get_rel(table as RelId)
            .cloned()
            .unwrap_or_else(BTreeMap::new),
    );

    Box::into_raw(Box::new(res))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delta_enumerate(
    delta: *const DeltaMap<DDValue>,
    cb: Option<ExternCCallback>,
    cb_arg: libc::uintptr_t,
) {
    if let Some(f) = cb {
        for (table_id, table_data) in (&*delta).as_ref().iter() {
            for (val, weight) in table_data.iter() {
                f(
                    cb_arg,
                    *table_id as libc::size_t,
                    &val.clone().into_record(),
                    *weight as libc::ssize_t,
                );
            }
        }
    };
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delta_clear_table(
    delta: *mut DeltaMap<DDValue>,
    table: libc::size_t,
) {
    if !delta.is_null() {
        (&mut *delta).clear_rel(table as RelId);
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delta_remove_table(
    delta: *mut DeltaMap<DDValue>,
    table: libc::size_t,
) -> *mut DeltaMap<DDValue> {
    if !delta.is_null() {
        Box::into_raw(Box::new(DeltaMap::singleton(
            table,
            (&mut *delta).clear_rel(table as RelId),
        )))
    } else {
        ptr::null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delta_clear(delta: *mut DeltaMap<DDValue>) {
    if !delta.is_null() {
        (&mut *delta).as_mut().clear();
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delta_union(
    delta: *mut DeltaMap<DDValue>,
    new_delta: *const DeltaMap<DDValue>,
) {
    if !delta.is_null() && !new_delta.is_null() {
        for (table_id, table_data) in (&*new_delta).as_ref().iter() {
            for (val, weight) in table_data.iter() {
                (&mut *delta).update(*table_id, val, *weight);
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_free_delta(delta: *mut DeltaMap<DDValue>) {
    if !delta.is_null() {
        // Deallocate the DeltaMap
        Box::from_raw(delta);
    }
}
