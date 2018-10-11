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

pub mod valmap;
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
