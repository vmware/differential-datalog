//! OVSDB JSON interface to RunningProgram

use differential_datalog::program::*;
use std::ffi::{CStr};
use std::os::raw::{c_char, c_int};
use super::{Value, updcmd2upd};
use ddlog_ovsdb_adapter::*;

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
pub extern "C" fn datalog_example_apply_ovsdb_updates(prog: *mut RunningProgram<Value>,
                                                      prefix: *const c_char,
                                                      updates: *const c_char) -> c_int {
    match apply_updates(prog, prefix, updates) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{}", e.as_str());
            -1
        }
    }
}

fn apply_updates(prog: *mut RunningProgram<Value>, prefix: *const c_char, updates_str: *const c_char) -> Result<(), String> {
    let prefix: &str = unsafe{ CStr::from_ptr(prefix) }.to_str()
        .map_err(|e|format!("invalid UTF8 string in prefix: {}", e))?;
    let updates_str: &str = unsafe{ CStr::from_ptr(updates_str) }.to_str()
        .map_err(|e|format!("invalid UTF8 string in prefix: {}", e))?;
    let commands = cmds_from_table_updates_str(prefix, updates_str)?;
    let updates: Result<Vec<Update<Value>>, String> = commands.iter().map(|c|updcmd2upd(c)).collect();
    (unsafe{&mut *prog}).apply_updates(updates?)
}
