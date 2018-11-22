//! OVSDB JSON interface to RunningProgram

use differential_datalog::program::*;
use differential_datalog::record::IntoRecord;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use super::{Value, updcmd2upd};
use ddlog_ovsdb_adapter::*;
use super::valmap;
use std::sync;
use std::ptr;
use super::{HDDlog, output_relname_to_id};

/// Parse OVSDB JSON <table-updates> value into DDlog commands; apply commands to a DDlog program.
///
/// Must be called in the context of a transaction.
///
/// `prefix` contains is the prefix to be added to JSON table names, e.g, `OVN_Southbound_` or
/// `OVN_Northbound_` for OVN southbound and northbound database updates.
///
/// `updates` is the JSON string, e.g.,
/// ```
/// {"Logical_Switch":{"ffe8d84e-b4a0-419e-b865-19f151eed878":{"new":{"acls":["set",[]],"dns_records":["set",[]],"external_ids":["map",[]],"load_balancer":["set",[]],"name":"lsw0","other_config":["map",[]],"ports":["set",[]],"qos_rules":["set",[]]}}}}
/// ```
///
#[no_mangle]
pub unsafe extern "C" fn datalog_example_apply_ovsdb_updates(
    prog: *const HDDlog,
    prefix: *const c_char,
    updates: *const c_char) -> c_int
{
    if prog.is_null() || prefix.is_null() || updates.is_null() {
        return -1;
    };
    let prog = sync::Arc::from_raw(prog);
    let res = apply_updates(&mut prog.0.lock().unwrap(), prefix, updates).map(|_|0).unwrap_or_else(|e|{
        eprintln!("datalog_example_apply_ovsdb_updates(): error: {}", e);
        -1
    });
    sync::Arc::into_raw(prog);
    res
}

fn apply_updates(prog: &mut RunningProgram<Value>, prefix: *const c_char, updates_str: *const c_char) -> Result<(), String>
{
    let prefix: &str = unsafe{ CStr::from_ptr(prefix) }.to_str()
        .map_err(|e|format!("invalid UTF8 string in prefix: {}", e))?;
    let updates_str: &str = unsafe{ CStr::from_ptr(updates_str) }.to_str()
        .map_err(|e|format!("invalid UTF8 string in prefix: {}", e))?;
    let commands = cmds_from_table_updates_str(prefix, updates_str)?;
    let updates: Result<Vec<Update<Value>>, String> = commands.iter().map(|c|updcmd2upd(c)).collect();
    prog.apply_updates(updates?)
}


/// Dump OVSDB Delta-Plus table as a sequence of OVSDB Insert commands in JSON format.
///
/// On success, returns `0` and stores a pointer to JSON string in `json`.  This pointer must be
/// later deallocated by calling `datalog_example_free_json()`
///
/// On error, returns a negative number and writes error message to stderr.
#[no_mangle]
pub extern "C" fn datalog_example_dump_ovsdb_deltaplus_table(prog:  *const HDDlog,
                                                             table: *const c_char,
                                                             json:  *mut *mut c_char) -> c_int {
    if json.is_null() || prog.is_null() || table.is_null() {
        return -1;
    };
    let prog = unsafe {sync::Arc::from_raw(prog)};
    let res = match dump_deltaplus_table(&mut prog.1.lock().unwrap(), table) {
        Ok(jinserts) => {
            unsafe { *json = jinserts.into_raw() };
            0
        },
        Err(e) => {
            eprintln!("datalog_example_dump_ovsdb_deltaplus_table(): error: {}", e);
            -1
        }
    };
    sync::Arc::into_raw(prog);
    res
}

fn dump_deltaplus_table(db: &mut valmap::ValMap, table: *const c_char) -> Result<CString, String> {
    let table_str = unsafe{ CStr::from_ptr(table) }.to_str().map_err(|e| format!("{}", e))?;
    let relid = output_relname_to_id(table_str).ok_or_else(||format!("unknown output relation {}", table_str))?;
    let cmds: Result<Vec<String>, String> =
        db.get_rel(relid as RelId)
          .iter().map(|v| record_into_insert_str(v.clone().into_record())).collect();
    Ok(unsafe{ CString::from_vec_unchecked(cmds?.join(",").into_bytes()) } )
}

/// Dump OVSDB Delta-Minus table as a sequence of OVSDB Delete commands in JSON format.
///
/// On success, returns `0` and stores a pointer to JSON string in `json`.  This pointer must be
/// later deallocated by calling `datalog_example_free_json()`
///
/// On error, returns a negative number and writes error message to stderr.
#[no_mangle]
pub extern "C" fn datalog_example_dump_ovsdb_deltaminus_table(prog:  *const HDDlog,
                                                              table: *const c_char,
                                                              json:  *mut *mut c_char) -> c_int {
    if json.is_null() || prog.is_null() || table.is_null() {
        return -1;
    };
    let prog = unsafe {sync::Arc::from_raw(prog)};
    let res = match dump_deltaminus_table(&mut prog.1.lock().unwrap(), table) {
        Ok(jdeletes) => {
            unsafe { *json = jdeletes.into_raw() };
            0
        },
        Err(e) => {
            eprintln!("datalog_example_dump_ovsdb_deltaplus_table(): error: {}", e);
            -1
        }
    };
    sync::Arc::into_raw(prog);
    res
}

fn dump_deltaminus_table(db: &mut valmap::ValMap, table: *const c_char) -> Result<CString, String> {
    let table_str = unsafe{ CStr::from_ptr(table) }.to_str().map_err(|e| format!("{}", e))?;
    let relid = output_relname_to_id(table_str).ok_or_else(||format!("unknown output relation {}", table_str))?;
    let cmds: Result<Vec<String>, String> =
        db.get_rel(relid as RelId)
          .iter().map(|v| record_into_delete_str(v.clone().into_record())).collect();
    Ok(unsafe{ CString::from_vec_unchecked(cmds?.join(",").into_bytes()) } )
}

/// Dump OVSDB Delta-Update table as a sequence of OVSDB Update commands in JSON format.
///
/// On success, returns `0` and stores a pointer to JSON string in `json`.  This pointer must be
/// later deallocated by calling `datalog_example_free_json()`
///
/// On error, returns a negative number and writes error message to stderr.
#[no_mangle]
pub extern "C" fn datalog_example_dump_ovsdb_deltupdate_table(prog:  *const HDDlog,
                                                              table: *const c_char,
                                                              json:  *mut *mut c_char) -> c_int {
    if json.is_null() || prog.is_null() || table.is_null() {
        return -1;
    };
    let prog = unsafe {sync::Arc::from_raw(prog)};
    let res = match dump_deltaupdate_table(&mut prog.1.lock().unwrap(), table) {
        Ok(jupdates) => {
            unsafe { *json = jupdates.into_raw() };
            0
        },
        Err(e) => {
            eprintln!("datalog_example_dump_ovsdb_deltaupdate_table(): error: {}", e);
            -1
        }
    };
    sync::Arc::into_raw(prog);
    res
}

fn dump_deltaupdate_table(db: &mut valmap::ValMap, table: *const c_char) -> Result<CString, String> {
    let table_str = unsafe{ CStr::from_ptr(table) }.to_str().map_err(|e| format!("{}", e))?;
    let relid = output_relname_to_id(table_str).ok_or_else(||format!("unknown output relation {}", table_str))?;
    let cmds: Result<Vec<String>, String> =
        db.get_rel(relid as RelId)
          .iter().map(|v| record_into_update_str(v.clone().into_record())).collect();
    Ok(unsafe{ CString::from_vec_unchecked(cmds?.join(",").into_bytes()) } )
}

/// Deallocates strings returned by other functions in this API.
#[no_mangle]
pub extern "C" fn datalog_example_free_json(str: *mut c_char) {
    if str.is_null() { return; }
    let cstr = unsafe{CString::from_raw(str)};
}
