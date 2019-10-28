//! An untyped representation of DDlog values and database update commands.

use libc;
use num::{BigInt, BigUint, ToPrimitive};
use std::borrow::Cow;
use std::collections::{btree_map, BTreeMap, BTreeSet};
use std::ffi::{CStr, CString};
use std::fmt;
use std::fmt::Write;
use std::iter::FromIterator;
use std::os::raw;
use std::ptr::{null, null_mut};
use std::slice;
use std::string::ToString;
use std::vec;

use serde::{Deserialize, Serialize};

pub type Name = Cow<'static, str>;

/* Rust's implementation of `Debug::fmt` for `str` incorrectly escapes
 * single quotes, e.g., "isn't" becomes "isn\'t".  To get around this,
 * I copied Rust's implementation and modified it to handle single quotes
 * as a special case. */
pub fn format_ddlog_str(s: &str, f: &mut fmt::Formatter) -> fmt::Result {
    //write!(f, "{:?}", s),
    f.write_char('"')?;
    let mut from = 0;
    for (i, c) in s.char_indices() {
        let esc = c.escape_debug();
        if esc.len() != 1 && c != '\'' {
            f.write_str(&s[from..i])?;
            for c in esc {
                f.write_char(c)?;
            }
            from = i + c.len_utf8();
        }
    }
    f.write_str(&s[from..])?;
    f.write_char('"')
}

/// `enum Record` represents an arbitrary DDlog value.
///
/// It relies on strings to store constructor and field names.  When manufacturing an instance of
/// `Record` from a typed DDlog value, strings can be cheap `&'static str`'s.  When manufacturing an
/// instance from some external representation, e.g., JSON, one needs to use `String`'s instead.  To
/// accommodate both options, `Record` uses `Cow` to store names.
///
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Record {
    Bool(bool),
    Int(BigInt),
    String(String),
    Tuple(Vec<Record>),
    Array(CollectionKind, Vec<Record>),
    PosStruct(Name, Vec<Record>),
    NamedStruct(Name, Vec<(Name, Record)>),
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Record::Bool(true) => write!(f, "true"),
            Record::Bool(false) => write!(f, "false"),
            Record::Int(i) => i.fmt(f),
            Record::String(s) => format_ddlog_str(s.as_ref(), f),
            Record::Tuple(recs) => {
                write!(f, "(")?;
                let len = recs.len();
                for (i, r) in recs.iter().enumerate() {
                    if i == len - 1 {
                        write!(f, "{}", r)?;
                    } else {
                        write!(f, "{}, ", r)?;
                    }
                }
                write!(f, ")")
            }
            Record::Array(_, recs) => {
                write!(f, "[")?;
                let len = recs.len();
                for (i, r) in recs.iter().enumerate() {
                    if i == len - 1 {
                        write!(f, "{}", r)?;
                    } else {
                        write!(f, "{}, ", r)?;
                    }
                }
                write!(f, "]")
            }
            Record::PosStruct(n, recs) => {
                write!(f, "{}{{", n)?;
                let len = recs.len();
                for (i, r) in recs.iter().enumerate() {
                    if i == len - 1 {
                        write!(f, "{}", r)?;
                    } else {
                        write!(f, "{}, ", r)?;
                    }
                }
                write!(f, "}}")
            }
            Record::NamedStruct(n, recs) => {
                write!(f, "{}{{", n)?;
                let len = recs.len();
                for (i, (fname, v)) in recs.iter().enumerate() {
                    if i == len - 1 {
                        write!(f, ".{} = {}", fname, v)?;
                    } else {
                        write!(f, ".{} = {}, ", fname, v)?;
                    }
                }
                write!(f, "}}")
            }
        }
    }
}

#[derive(Copy, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum CollectionKind {
    Unknown,
    Vector,
    Set,
    Map,
}

/// Relation can be identified by name (e.g., when parsing JSON or text)
/// or ID, which is more efficient if the caller bothered to convert
/// relation name to ID.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RelIdentifier {
    RelName(Name),
    RelId(usize),
}

impl fmt::Display for RelIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RelIdentifier::RelName(rname) => write!(f, "{}", rname),
            RelIdentifier::RelId(rid) => write!(f, "{}", rid),
        }
    }
}

/// Four types of DDlog relation update commands that match the `Update` enum in `program.rs`
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UpdCmd {
    Insert(RelIdentifier, Record),
    Delete(RelIdentifier, Record),
    DeleteKey(RelIdentifier, Record),
    Modify(RelIdentifier, Record, Record),
}

/*
 * C API to Record and UpdCmd
 */

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_record(rec: *const Record) -> *mut raw::c_char {
    match rec.as_ref() {
        Some(rec) => match CString::new(rec.to_string()) {
            Ok(s) => s.into_raw(),
            Err(_) => null_mut(),
        },
        _ => null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_free(rec: *mut Record) {
    Box::from_raw(rec);
}

#[no_mangle]
pub extern "C" fn ddlog_bool(b: bool) -> *mut Record {
    Box::into_raw(Box::new(Record::Bool(b)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_bool(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Bool(_)) => true,
        _ => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_bool(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Bool(b)) => *b,
        _ => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_int(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Int(_)) => true,
        _ => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_int(v: *const raw::c_uchar, size: libc::size_t) -> *mut Record {
    Box::into_raw(Box::new(Record::Int(BigInt::from_signed_bytes_be(
        slice::from_raw_parts(v as *const u8, size as usize),
    ))))
}

/* `buf`        - buffer to store the big-endian byte representation of the integer value
 * `capacity`   - buffer capacity
 *
 * Return value: if `capacity` is 0, returns the minimal buffer capacity necessary to
 * represent the value otherwise returns the number of bytes stored in `buf` or `-1` if `buf`
 * is not big enough.
 */
#[no_mangle]
pub unsafe extern "C" fn ddlog_get_int(
    rec: *const Record,
    buf: *mut raw::c_uchar,
    capacity: libc::size_t,
) -> libc::ssize_t {
    match rec.as_ref() {
        Some(Record::Int(i)) => {
            let bytes = i.to_signed_bytes_be();
            if capacity == 0 {
                bytes.len() as libc::ssize_t
            } else if capacity >= bytes.len() {
                for (i, b) in bytes.iter().enumerate() {
                    if let Some(p) = buf.add(i).as_mut() {
                        *p = *b;
                    };
                }
                bytes.len() as libc::ssize_t
            } else {
                -1
            }
        }
        _ => 0,
    }
}

/* Determines the fewest bits necessary to express the integer value, not including the sign.
 */
#[no_mangle]
pub unsafe extern "C" fn ddlog_int_bits(rec: *const Record) -> libc::size_t {
    match rec.as_ref() {
        Some(Record::Int(bigint)) => bigint.bits() as libc::size_t,
        _ => 0,
    }
}

#[no_mangle]
pub extern "C" fn ddlog_u64(v: u64) -> *mut Record {
    Box::into_raw(Box::new(Record::Int(BigInt::from(v))))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_u64(rec: *const Record) -> u64 {
    match rec.as_ref() {
        Some(Record::Int(i)) => i.to_u64().unwrap_or(0),
        _ => 0,
    }
}

#[no_mangle]
pub extern "C" fn ddlog_i64(v: i64) -> *mut Record {
    Box::into_raw(Box::new(Record::Int(BigInt::from(v))))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_i64(rec: *const Record) -> i64 {
    match rec.as_ref() {
        Some(Record::Int(i)) => i.to_i64().unwrap_or(0),
        _ => 0,
    }
}

#[no_mangle]
pub extern "C" fn ddlog_u128(v: u128) -> *mut Record {
    Box::into_raw(Box::new(Record::Int(BigInt::from(v))))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_u128(rec: *const Record) -> u128 {
    match rec.as_ref() {
        Some(Record::Int(i)) => i.to_u128().unwrap_or(0),
        _ => 0,
    }
}

#[no_mangle]
pub extern "C" fn ddlog_i128(v: i128) -> *mut Record {
    Box::into_raw(Box::new(Record::Int(BigInt::from(v))))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_i128(rec: *const Record) -> i128 {
    match rec.as_ref() {
        Some(Record::Int(i)) => i.to_i128().unwrap_or(0),
        _ => 0,
    }
}

/// Returns NULL if s is not a valid UTF8 string.
#[no_mangle]
pub unsafe extern "C" fn ddlog_string(s: *const raw::c_char) -> *mut Record {
    let s = match CStr::from_ptr(s).to_str() {
        Ok(s) => s,
        Err(_) => {
            return null_mut();
        }
    };
    Box::into_raw(Box::new(Record::String(s.to_owned())))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_string(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::String(_)) => true,
        _ => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_strlen(rec: *const Record) -> libc::size_t {
    match rec.as_ref() {
        Some(Record::String(s)) => s.len() as libc::size_t,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_str_non_nul(rec: *const Record) -> *const raw::c_char {
    match rec.as_ref() {
        Some(Record::String(s)) => s.as_ptr() as *const raw::c_char,
        _ => null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_tuple(fields: *const *mut Record, len: libc::size_t) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    Box::into_raw(Box::new(Record::Tuple(fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_tuple(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Tuple(_)) => true,
        _ => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_tuple_size(rec: *const Record) -> libc::size_t {
    match rec.as_ref() {
        Some(Record::Tuple(recs)) => recs.len() as libc::size_t,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_tuple_field(
    rec: *const Record,
    idx: libc::size_t,
) -> *const Record {
    match rec.as_ref() {
        Some(Record::Tuple(recs)) => recs
            .get(idx)
            .map(|r| r as *const Record)
            .unwrap_or(null_mut()),
        _ => null_mut(),
    }
}

/// Convenience method to create a 2-tuple.
#[no_mangle]
pub unsafe extern "C" fn ddlog_pair(v1: *mut Record, v2: *mut Record) -> *mut Record {
    let v1 = Box::from_raw(v1);
    let v2 = Box::from_raw(v2);
    Box::into_raw(Box::new(Record::Tuple(vec![*v1, *v2])))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_tuple_push(tup: *mut Record, rec: *mut Record) {
    let rec = Box::from_raw(rec);
    let mut tup = Box::from_raw(tup);
    if let Record::Tuple(recs) = tup.as_mut() {
        recs.push(*rec)
    };
    Box::into_raw(tup);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_vector(
    fields: *const *mut Record,
    len: libc::size_t,
) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    Box::into_raw(Box::new(Record::Array(CollectionKind::Vector, fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_vector(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Vector, _)) => true,
        _ => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_vector_size(rec: *const Record) -> libc::size_t {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Vector, recs)) => recs.len() as libc::size_t,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_vector_elem(
    rec: *const Record,
    idx: libc::size_t,
) -> *const Record {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Vector, recs)) => recs
            .get(idx)
            .map(|r| r as *const Record)
            .unwrap_or(null_mut()),
        _ => null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_vector_push(vec: *mut Record, rec: *mut Record) {
    let rec = Box::from_raw(rec);
    let mut vec = Box::from_raw(vec);
    if let Record::Array(CollectionKind::Vector, recs) = vec.as_mut() {
        recs.push(*rec)
    };
    Box::into_raw(vec);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_set(fields: *const *mut Record, len: libc::size_t) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    Box::into_raw(Box::new(Record::Array(CollectionKind::Set, fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_set(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Set, _)) => true,
        _ => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_set_size(rec: *const Record) -> libc::size_t {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Set, recs)) => recs.len() as libc::size_t,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_set_elem(
    rec: *const Record,
    idx: libc::size_t,
) -> *const Record {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Set, recs)) => recs
            .get(idx)
            .map(|r| r as *const Record)
            .unwrap_or(null_mut()),
        _ => null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_set_push(set: *mut Record, rec: *mut Record) {
    let rec = Box::from_raw(rec);
    let mut set = Box::from_raw(set);
    if let Record::Array(CollectionKind::Map, recs) = set.as_mut() {
        recs.push(*rec)
    };
    Box::into_raw(set);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_map(fields: *const *mut Record, len: libc::size_t) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    Box::into_raw(Box::new(Record::Array(CollectionKind::Map, fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_map(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Map, _)) => true,
        _ => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_map_size(rec: *const Record) -> libc::size_t {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Map, recs)) => recs.len() as libc::size_t,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_map_key(rec: *const Record, idx: libc::size_t) -> *const Record {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Map, recs)) => recs
            .get(idx)
            .map(|r| match r {
                Record::Tuple(kv) if kv.len() == 2 => kv.get(0).unwrap() as *const Record,
                _ => null_mut(),
            })
            .unwrap_or(null_mut()),
        _ => null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_map_val(rec: *const Record, idx: libc::size_t) -> *const Record {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Map, recs)) => recs
            .get(idx)
            .map(|r| match r {
                Record::Tuple(kv) if kv.len() == 2 => kv.get(1).unwrap() as *const Record,
                _ => null_mut(),
            })
            .unwrap_or(null_mut()),
        _ => null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_map_push(map: *mut Record, key: *mut Record, val: *mut Record) {
    let key = Box::from_raw(key);
    let val = Box::from_raw(val);
    let tup = Record::Tuple(vec![*key, *val]);
    let mut map = Box::from_raw(map);
    if let Record::Array(CollectionKind::Map, recs) = map.as_mut() {
        recs.push(tup)
    };
    Box::into_raw(map);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_struct(
    constructor: *const raw::c_char,
    fields: *const *mut Record,
    len: libc::size_t,
) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    let constructor = match CStr::from_ptr(constructor).to_str() {
        Ok(s) => s,
        Err(_) => {
            return null_mut();
        }
    };
    Box::into_raw(Box::new(Record::PosStruct(
        Cow::from(constructor.to_owned()),
        fields,
    )))
}

/// Similar to `ddlog_struct()`, but expects `constructor` to be static string.
/// Doesn't allocate memory for a local copy of the string.
#[no_mangle]
pub unsafe extern "C" fn ddlog_struct_static_cons(
    constructor: *const raw::c_char,
    fields: *const *mut Record,
    len: libc::size_t,
) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    let constructor = match CStr::from_ptr(constructor).to_str() {
        Ok(s) => s,
        Err(_) => {
            return null_mut();
        }
    };
    Box::into_raw(Box::new(Record::PosStruct(Cow::from(constructor), fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_struct(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::PosStruct(_, _)) => true,
        Some(Record::NamedStruct(_, _)) => true,
        _ => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_struct_field(
    rec: *const Record,
    idx: libc::size_t,
) -> *const Record {
    match rec.as_ref() {
        Some(Record::PosStruct(_, fields)) => fields
            .get(idx)
            .map(|r| r as *const Record)
            .unwrap_or(null_mut()),
        Some(Record::NamedStruct(_, fields)) => fields
            .get(idx)
            .map(|(_, r)| r as *const Record)
            .unwrap_or(null_mut()),
        _ => null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_constructor_non_null(
    rec: *const Record,
    len: *mut libc::size_t,
) -> *const raw::c_char {
    match rec.as_ref() {
        Some(Record::PosStruct(cons, _)) => {
            *len = cons.len();
            cons.as_ref().as_ptr() as *const raw::c_char
        }
        Some(Record::NamedStruct(cons, _)) => {
            *len = cons.len();
            cons.as_ref().as_ptr() as *const raw::c_char
        }
        _ => {
            *len = 0;
            null()
        }
    }
}

unsafe fn mk_record_vec(fields: *const *mut Record, len: libc::size_t) -> Vec<Record> {
    let mut tfields = Vec::with_capacity(len as usize);
    for i in 0..len {
        tfields.push(*Box::from_raw(*fields.add(i)));
    }
    tfields
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_insert_cmd(table: libc::size_t, rec: *mut Record) -> *mut UpdCmd {
    let rec = Box::from_raw(rec);
    Box::into_raw(Box::new(UpdCmd::Insert(RelIdentifier::RelId(table), *rec)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delete_val_cmd(
    table: libc::size_t,
    rec: *mut Record,
) -> *mut UpdCmd {
    let rec = Box::from_raw(rec);
    Box::into_raw(Box::new(UpdCmd::Delete(RelIdentifier::RelId(table), *rec)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delete_key_cmd(
    table: libc::size_t,
    rec: *mut Record,
) -> *mut UpdCmd {
    let rec = Box::from_raw(rec);
    Box::into_raw(Box::new(UpdCmd::DeleteKey(
        RelIdentifier::RelId(table),
        *rec,
    )))
}

/*
 * Traits for converting ddlog `Values` to/from Record's.
 */

/// `Mutator` trait represents an object that can be used to mutate a value (e.g., change some of
/// its fields).
pub trait Mutator<V>: fmt::Display {
    /// Consumes a value and returns an updated value.
    fn mutate(&self, v: &mut V) -> Result<(), String>;
}

/// `FromRecord` trait.  For types that can be converted from cmd_parser::Record type
pub trait FromRecord: Sized {
    fn from_record(val: &Record) -> Result<Self, String>;
}

pub trait IntoRecord {
    fn into_record(self) -> Record;
}

/*
 * Trait implementations for built-in types.
 */

impl FromRecord for u8 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_u8() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to u8", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl Mutator<u8> for Record {
    fn mutate(&self, v: &mut u8) -> Result<(), String> {
        *v = u8::from_record(self)?;
        Ok(())
    }
}

impl IntoRecord for u8 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl FromRecord for u16 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_u16() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to u16", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for u16 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl Mutator<u16> for Record {
    fn mutate(&self, v: &mut u16) -> Result<(), String> {
        *v = u16::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for u32 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_u32() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to u32", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for u32 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl Mutator<u32> for Record {
    fn mutate(&self, v: &mut u32) -> Result<(), String> {
        *v = u32::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for u64 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_u64() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to u64", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for u64 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl Mutator<u64> for Record {
    fn mutate(&self, v: &mut u64) -> Result<(), String> {
        *v = u64::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for u128 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_u128() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to u128", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for u128 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl Mutator<u128> for Record {
    fn mutate(&self, v: &mut u128) -> Result<(), String> {
        *v = u128::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for i8 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_i8() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to i8", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl Mutator<i8> for Record {
    fn mutate(&self, v: &mut i8) -> Result<(), String> {
        *v = i8::from_record(self)?;
        Ok(())
    }
}

impl IntoRecord for i8 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl FromRecord for i16 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_i16() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to i16", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for i16 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl Mutator<i16> for Record {
    fn mutate(&self, v: &mut i16) -> Result<(), String> {
        *v = i16::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for i32 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_i32() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to i32", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for i32 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl Mutator<i32> for Record {
    fn mutate(&self, v: &mut i32) -> Result<(), String> {
        *v = i32::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for i64 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_i64() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to i64", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for i64 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl Mutator<i64> for Record {
    fn mutate(&self, v: &mut i64) -> Result<(), String> {
        *v = i64::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for i128 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_i128() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to i128", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for i128 {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl Mutator<i128> for Record {
    fn mutate(&self, v: &mut i128) -> Result<(), String> {
        *v = i128::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for BigInt {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => Result::Ok(i.clone()),
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for BigInt {
    fn into_record(self) -> Record {
        Record::Int(self)
    }
}

impl Mutator<BigInt> for Record {
    fn mutate(&self, v: &mut BigInt) -> Result<(), String> {
        *v = BigInt::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for BigUint {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => match i.to_biguint() {
                Some(x) => Result::Ok(x),
                None => Result::Err(format!("cannot convert {} to BigUint", i)),
            },
            v => Result::Err(format!("not an int {:?}", *v)),
        }
    }
}

impl IntoRecord for BigUint {
    fn into_record(self) -> Record {
        Record::Int(BigInt::from(self))
    }
}

impl Mutator<BigUint> for Record {
    fn mutate(&self, v: &mut BigUint) -> Result<(), String> {
        *v = BigUint::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for bool {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Bool(b) => Result::Ok(*b),
            v => Result::Err(format!("not a bool {:?}", *v)),
        }
    }
}

impl IntoRecord for bool {
    fn into_record(self) -> Record {
        Record::Bool(self)
    }
}

impl Mutator<bool> for Record {
    fn mutate(&self, v: &mut bool) -> Result<(), String> {
        *v = bool::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for String {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::String(s) => Result::Ok(s.clone()),
            v => Result::Err(format!("not a string {:?}", *v)),
        }
    }
}

impl IntoRecord for String {
    fn into_record(self) -> Record {
        Record::String(self)
    }
}

impl Mutator<String> for Record {
    fn mutate(&self, v: &mut String) -> Result<(), String> {
        *v = String::from_record(self)?;
        Ok(())
    }
}

impl FromRecord for () {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.is_empty() => Ok(()),
            v => Result::Err(format!("not a 0-tuple {:?}", *v)),
        }
    }
}

impl IntoRecord for () {
    fn into_record(self) -> Record {
        Record::Tuple(vec![])
    }
}

impl Mutator<()> for Record {
    fn mutate(&self, _v: &mut ()) -> Result<(), String> {
        Ok(())
    }
}

macro_rules! decl_tuple_from_record {
    ( $n:tt, $( $t:tt , $i:tt),+ ) => {
        impl <$($t: FromRecord),*> FromRecord for ($($t),*) {
            fn from_record(val: &Record) -> Result<Self, String> {
                match val {
                    $crate::record::Record::Tuple(args) if args.len() == $n => {
                        Ok(( $($t::from_record(&args[$i])?),*))
                    },
                    v => { Result::Err(format!("not a {}-tuple {:?}", $n, *v)) }
                }
            }
        }

        impl <$($t: IntoRecord),*> IntoRecord for ($($t),*) {
            fn into_record(self) -> $crate::record::Record {
                Record::Tuple(vec![$(self.$i.into_record()),*])
            }
        }

        impl <$($t: FromRecord),*> Mutator<($($t),*)> for Record {
            fn mutate(&self, v: &mut ($($t),*)) -> Result<(), String> {
                *v = <($($t),*)>::from_record(self)?;
                Ok(())
            }
        }
    };
}

decl_tuple_from_record!(2, T0, 0, T1, 1);
decl_tuple_from_record!(3, T0, 0, T1, 1, T2, 2);
decl_tuple_from_record!(4, T0, 0, T1, 1, T2, 2, T3, 3);
decl_tuple_from_record!(5, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4);
decl_tuple_from_record!(6, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5);
decl_tuple_from_record!(7, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6);
decl_tuple_from_record!(8, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7);
decl_tuple_from_record!(9, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8);
decl_tuple_from_record!(10, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9);
decl_tuple_from_record!(
    11, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10
);
decl_tuple_from_record!(
    12, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11
);
decl_tuple_from_record!(
    13, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11,
    T12, 12
);
decl_tuple_from_record!(
    14, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11,
    T12, 12, T13, 13
);
decl_tuple_from_record!(
    15, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11,
    T12, 12, T13, 13, T14, 14
);
decl_tuple_from_record!(
    16, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11,
    T12, 12, T13, 13, T14, 14, T15, 15
);
decl_tuple_from_record!(
    17, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11,
    T12, 12, T13, 13, T14, 14, T15, 15, T16, 16
);
decl_tuple_from_record!(
    18, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11,
    T12, 12, T13, 13, T14, 14, T15, 15, T16, 16, T17, 17
);
decl_tuple_from_record!(
    19, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11,
    T12, 12, T13, 13, T14, 14, T15, 15, T16, 16, T17, 17, T18, 18
);
decl_tuple_from_record!(
    20, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11,
    T12, 12, T13, 13, T14, 14, T15, 15, T16, 16, T17, 17, T18, 18, T19, 19
);

impl<T: FromRecord> FromRecord for vec::Vec<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Array(_, args) => Result::from_iter(args.iter().map(|x| T::from_record(x))),
            v => {
                T::from_record(v).map(|x| vec![x])
                //Result::Err(format!("not an array {:?}", *v))
            }
        }
    }
}

impl<T: IntoRecord> IntoRecord for vec::Vec<T> {
    fn into_record(self) -> Record {
        Record::Array(
            CollectionKind::Vector,
            self.into_iter().map(IntoRecord::into_record).collect(),
        )
    }
}

impl<T: FromRecord> Mutator<vec::Vec<T>> for Record {
    fn mutate(&self, v: &mut vec::Vec<T>) -> Result<(), String> {
        *v = <vec::Vec<T>>::from_record(self)?;
        Ok(())
    }
}

macro_rules! decl_arr_from_record {
    ( $i:tt ) => {
        impl<T: FromRecord + Default> FromRecord for [T; $i] {
            fn from_record(val: &Record) -> Result<Self, String> {
                let vec = Vec::from_record(val)?;
                let mut arr = <[T; $i]>::default();
                if vec.len() != $i {
                    return Result::Err(format!(
                        "cannot convert {:?} to array of length {}",
                        *val, $i
                    ));
                };
                let mut idx = 0;
                for v in vec.into_iter() {
                    arr[idx] = v;
                    idx += 1;
                }
                Ok(arr)
            }
        }

        impl<T: IntoRecord + Clone> IntoRecord for [T; $i] {
            fn into_record(self) -> Record {
                Record::Array(
                    CollectionKind::Vector,
                    self.into_iter()
                        .map(|x: &T| (*x).clone().into_record())
                        .collect(),
                )
            }
        }

        impl<T: FromRecord + Default> Mutator<[T; $i]> for Record {
            fn mutate(&self, v: &mut [T; $i]) -> Result<(), String> {
                *v = <[T; $i]>::from_record(self)?;
                Ok(())
            }
        }
    };
}

decl_arr_from_record!(1);
decl_arr_from_record!(2);
decl_arr_from_record!(3);
decl_arr_from_record!(4);
decl_arr_from_record!(5);
decl_arr_from_record!(6);
decl_arr_from_record!(7);
decl_arr_from_record!(8);
decl_arr_from_record!(9);
decl_arr_from_record!(10);
decl_arr_from_record!(11);
decl_arr_from_record!(12);
decl_arr_from_record!(13);
decl_arr_from_record!(14);
decl_arr_from_record!(15);
decl_arr_from_record!(16);
decl_arr_from_record!(17);
decl_arr_from_record!(18);
decl_arr_from_record!(19);
decl_arr_from_record!(20);
decl_arr_from_record!(21);
decl_arr_from_record!(22);
decl_arr_from_record!(23);
decl_arr_from_record!(24);
decl_arr_from_record!(25);
decl_arr_from_record!(26);
decl_arr_from_record!(27);
decl_arr_from_record!(28);
decl_arr_from_record!(29);
decl_arr_from_record!(30);
decl_arr_from_record!(31);
decl_arr_from_record!(32);

impl<K: FromRecord + Ord, V: FromRecord> FromRecord for BTreeMap<K, V> {
    fn from_record(val: &Record) -> Result<Self, String> {
        vec::Vec::from_record(val).map(BTreeMap::from_iter)
    }
}

impl<K: IntoRecord + Ord, V: IntoRecord> IntoRecord for BTreeMap<K, V> {
    fn into_record(self) -> Record {
        Record::Array(
            CollectionKind::Map,
            self.into_iter().map(IntoRecord::into_record).collect(),
        )
    }
}

/* Map update semantics is that the update contains keys that are in one of the maps but not the
 * other, plus keys that are in both maps but with different values. */
impl<K: FromRecord + Ord, V: FromRecord + PartialEq> Mutator<BTreeMap<K, V>> for Record {
    fn mutate(&self, map: &mut BTreeMap<K, V>) -> Result<(), String> {
        let upd = <BTreeMap<K, V>>::from_record(self)?;
        for (k, v) in upd.into_iter() {
            match map.entry(k) {
                btree_map::Entry::Vacant(ve) => {
                    /* key not in map -- insert */
                    ve.insert(v);
                }
                btree_map::Entry::Occupied(mut oe) => {
                    if *oe.get() == v {
                        /* key in map with the same value -- delete */
                        oe.remove_entry();
                    } else {
                        /* key in map, different value -- set new value */
                        oe.insert(v);
                    }
                }
            }
        }
        Ok(())
    }
}

impl<T: FromRecord + Ord> FromRecord for BTreeSet<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        vec::Vec::from_record(val).map(BTreeSet::from_iter)
    }
}

impl<T: IntoRecord + Ord> IntoRecord for BTreeSet<T> {
    fn into_record(self) -> Record {
        Record::Array(
            CollectionKind::Set,
            self.into_iter().map(IntoRecord::into_record).collect(),
        )
    }
}

/* Set update semantics: update contains values that are in one of the sets but not the
 * other. */
impl<T: FromRecord + Ord> Mutator<BTreeSet<T>> for Record {
    fn mutate(&self, set: &mut BTreeSet<T>) -> Result<(), String> {
        let upd = <BTreeSet<T>>::from_record(self)?;
        for v in upd.into_iter() {
            if !set.remove(&v) {
                set.insert(v);
            }
        }
        Ok(())
    }
}

/*
 * Use the following macros to generate `IntoRecord` and `Mutator` trait implementations for
 * user-defined structs and enums.
 *
 * TODO: implement similar macros for `FromRecord`.
 */

pub fn arg_extract<T: FromRecord + Default>(
    args: &[(Name, Record)],
    argname: &str,
) -> Result<T, String> {
    args.iter()
        .find(|(n, _)| *n == argname)
        .map_or_else(|| Ok(Default::default()), |(_, v)| T::from_record(v))
}

pub fn arg_find<'a>(args: &'a [(Name, Record)], argname: &str) -> Option<&'a Record> {
    args.iter().find(|(n, _)| *n == argname).map(|(_, v)| v)
}

#[macro_export]
macro_rules! decl_struct_into_record {
    ( $n:ident, <$( $targ:ident),*>, $( $arg:ident ),* ) => {
        impl <$($targ: $crate::record::IntoRecord),*> $crate::record::IntoRecord for $n<$($targ),*> {
            fn into_record(self) -> $crate::record::Record {
                $crate::record::Record::NamedStruct(borrow::Cow::from(stringify!($n)),vec![$((borrow::Cow::from(stringify!($arg)), self.$arg.into_record())),*])
            }
        }
    };
}

#[macro_export]
macro_rules! decl_record_mutator_struct {
    ( $n:ident, <$( $targ:ident),*>, $( $arg:ident : $type:ty),* ) => {
        impl<$($targ),*> $crate::record::Mutator<$n<$($targ),*>> for $crate::record::Record
            where $($crate::record::Record: $crate::record::Mutator<$targ>),*
        {
            fn mutate(&self, _x: &mut $n<$($targ),*>) -> Result<(), String> {
                match self {
                    $crate::record::Record::PosStruct(..) => {
                        return Err(format!("Cannot use positional struct as mutator"));
                    },
                    $crate::record::Record::NamedStruct(_, _args) => {
                        $(if let Some(arg_upd) = $crate::record::arg_find(_args, stringify!($arg)) {
                            <dyn $crate::record::Mutator<$type>>::mutate(arg_upd, &mut _x.$arg)?;
                          };)*
                    },
                    _ => {
                        return Result::Err(format!("not a struct {:?}", *self));
                    }
                };
                Ok(())
            }
        }
    };
}

#[macro_export]
macro_rules! decl_record_mutator_enum {
    ( $n:ident, <$( $targ:ident),*>, $($cons:ident {$( $arg:ident : $type:ty),*}),* ) => {
        impl<$($targ: $crate::record::FromRecord+Default),*> $crate::record::Mutator<$n<$($targ),*>> for $crate::record::Record
            where $($crate::record::Record: $crate::record::Mutator<$targ>),*
        {
            fn mutate(&self, x: &mut $n<$($targ),*>) -> Result<(), String> {
                match self {
                    $crate::record::Record::PosStruct(..) => {
                        return Err(format!("Cannot use positional struct as mutator"));
                    },
                    $crate::record::Record::NamedStruct(constr, args) => {
                        match (x, constr.as_ref()) {
                            $(
                                ($n::$cons{$($arg),*}, stringify!($cons)) => {
                                    $(
                                        if let Some(arg_upd) = $crate::record::arg_find(args, stringify!($arg)) {
                                            <dyn Mutator<$type>>::mutate(arg_upd, $arg)?;
                                        };
                                     )*
                                },

                            )*
                            (x, _) => {
                                *x = <$n<$($targ),*>>::from_record(self)?;
                            }
                        }
                    },
                    _ => {
                        return Result::Err(format!("not a struct {:?}", *self));
                    }
                };
                Ok(())
            }
        }
    };
}

#[macro_export]
macro_rules! decl_enum_into_record {
    ( $n:ident, <$( $targ:ident),*>, $($cons:ident {$($arg:ident),*} ),* ) => {
        impl <$($targ: $crate::record::IntoRecord),*> $crate::record::IntoRecord for $n<$($targ),*> {
            fn into_record(self) -> $crate::record::Record {
                match self {
                    $($n::$cons{$($arg),*} => $crate::record::Record::NamedStruct(borrow::Cow::from(stringify!($cons)), vec![$((borrow::Cow::from(stringify!($arg)), $arg.into_record())),*])),*
                }
            }
        }
    };
    ( $n:ident, <$( $targ:ident),*>, $($cons:ident ($($arg:ident),*) ),* ) => {
        impl <$($targ: $crate::record::IntoRecord),*> $crate::record::IntoRecord for $n<$($targ),*> {
            fn into_record(self) -> $crate::record::Record {
                match self {
                    $($n::$cons($($arg),*) => $crate::record::Record::NamedStruct(borrow::Cow::from(stringify!($cons)), vec![$((borrow::Cow::from(stringify!($arg)), $arg.into_record())),*])),*
                }
            }
        }
    };
}

#[macro_export]
macro_rules! decl_val_enum_into_record {
    ( $n:ident, <$( $targ:ident),*>, $($cons:ident {$arg:ident} ),* ) => {
        impl <$($targ: $crate::record::IntoRecord),*> $crate::record::IntoRecord for $n<$($targ),*> {
            fn into_record(self) -> $crate::record::Record {
                match self {
                    $($n::$cons{$arg} => $arg.into_record()),*
                }
            }
        }
    };
    ( $n:ident, <$( $targ:ident),*>, $($cons:ident ($arg:ident) ),* ) => {
        impl <$($targ: $crate::record::IntoRecord),*> $crate::record::IntoRecord for $n<$($targ),*> {
            fn into_record(self) -> $crate::record::Record {
                match self {
                    $($n::$cons($arg) => $arg.into_record()),*
                }
            }
        }
    };
}

#[macro_export]
macro_rules! decl_record_mutator_val_enum {
    ( $n:ident, <$( $targ:ident),*>, $($cons:ident ($type:ty)),* ) => {
        impl<$($targ: $crate::record::FromRecord+Default),*> $crate::record::Mutator<$n<$($targ),*>> for $crate::record::Record
            where $($crate::record::Record: $crate::record::Mutator<$targ>),*
        {
            fn mutate(&self, x: &mut $n<$($targ),*>) -> Result<(), String> {
                match self {
                    $crate::record::Record::PosStruct(..) => {
                        Err(format!("Cannot use positional struct as mutator"))
                    },
                    $crate::record::Record::NamedStruct(_, args) => {
                        match x {
                            $(
                                $n::$cons(v) => {
                                    <Mutator<$type>>::mutate(self, v)
                                }
                            ),*
                        }
                    },
                    _ => {
                        Result::Err(format!("not a struct {:?}", *self))
                    }
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    use num::bigint::ToBigInt;

    /// Test the `ddlog_dump_record` C API function.
    #[test]
    fn dump_record() {
        // Some very basic checks. Not all variants are covered at this
        // point.
        let checks = [
            (Record::Bool(true), "true"),
            (Record::Bool(false), "false"),
            (Record::Int(12345.to_bigint().unwrap()), "12345"),
            (Record::String("a-\0-byte".to_string()), "\"a-\\u{0}-byte\""),
        ];

        for check in &checks {
            let ptr = unsafe { ddlog_dump_record(&check.0) };
            assert!(!ptr.is_null());

            let actual = unsafe { CString::from_raw(ptr) };
            let expected = CString::new(check.1).unwrap();
            assert_eq!(actual, expected);
        }
    }
}
