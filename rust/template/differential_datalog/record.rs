//! An untyped representation of DDlog values and database update commands.

use num::{ToPrimitive, BigInt, BigUint};
use std::vec;
use std::collections::{btree_map, BTreeMap, BTreeSet};
use std::iter::FromIterator;
use std::borrow::Cow;
#[cfg(test)]
use std::borrow;
use std::ptr::{null_mut, null};
use std::ffi::CStr;
use std::os::raw::c_char;
use libc::size_t;
use std::fmt;

#[cfg(test)]
use num::bigint::{ToBigInt, ToBigUint};

pub type Name = Cow<'static, str>;

/// `enum Record` represents an arbitrary DDlog value.
///
/// It relies on string to store constructor and field names.  When manufacturing an instance of
/// `Record` from a typed DDlog, strings can be cheap `&'static str`'s.  When manufacturing an
/// instance from some external representation, e.g., JSON, one needs to use `String`'s instead.  To
/// accommodate both options, `Record` uses `Cow` to store names.
///
#[derive(Debug,PartialEq,Eq,Clone)]
pub enum Record {
    Bool(bool),
    Int(BigInt),
    String(String),
    Tuple(Vec<Record>),
    Array(CollectionKind, Vec<Record>),
    PosStruct(Name, Vec<Record>),
    NamedStruct(Name, Vec<(Name, Record)>)
}

#[derive(Debug,PartialEq,Eq,Clone)]
pub enum CollectionKind {
    Unknown,
    Vector,
    Set,
    Map
}


#[derive(Debug,PartialEq,Eq,Clone)]
pub enum RelIdentifier {
    RelName(Name),
    RelId(usize)
}

/*
 * Relation can be identified by name (e.g., when parsing JSON or text)
 * or ID, which is more efficient if the caller bothered to convert
 * relation name to ID.
 */
impl fmt::Display for RelIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RelIdentifier::RelName(rname) => write!(f, "{}", rname),
            RelIdentifier::RelId(rid) => write!(f, "{}", rid)
        }
    }
}

#[derive(Debug,PartialEq,Eq,Clone)]
pub enum UpdCmd {
    Insert (RelIdentifier, Record),
    Delete (RelIdentifier, Record),
    DeleteKey (RelIdentifier, Record),
    Modify (RelIdentifier, Record, Record)
}

/*
 * C API to Record and UpdCmd
 */


#[no_mangle]
pub unsafe extern "C" fn ddlog_free(rec: *mut Record)
{
    Box::from_raw(rec);
}

#[no_mangle]
pub extern "C" fn ddlog_bool(b: bool) -> *mut Record
{
    Box::into_raw(Box::new(Record::Bool(b)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_bool(rec: *const Record) -> bool
{
    match rec.as_ref() {
        Some(Record::Bool(_)) => true,
        _ => false
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_bool(rec: *const Record) -> bool
{
    match rec.as_ref() {
        Some(Record::Bool(b)) => *b,
        _ => false
    }
}

#[no_mangle]
pub extern "C" fn ddlog_u64(v: u64) -> *mut Record
{
    Box::into_raw(Box::new(Record::Int(BigInt::from(v))))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_int(rec: *const Record) -> bool
{
    match rec.as_ref() {
        Some(Record::Int(_)) => true,
        _ => false
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_u64(rec: *const Record) -> u64
{
    match rec.as_ref() {
        Some(Record::Int(i)) => i.to_u64().unwrap_or(0),
        _ => 0
    }
}


/// Returns NULL if s is not a valid UTF8 string.
#[no_mangle]
pub unsafe extern "C" fn ddlog_string(s: *const c_char) -> *mut Record
{
    let s = match CStr::from_ptr(s).to_str() {
        Ok(s) => s,
        Err(_) => {return null_mut();}
    };
    Box::into_raw(Box::new(Record::String(s.to_owned())))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_string(rec: *const Record) -> bool
{
    match rec.as_ref() {
        Some(Record::String(_)) => true,
        _ => false
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_strlen(rec: *const Record) -> size_t
{
    match rec.as_ref() {
        Some(Record::String(s)) => s.len() as size_t,
        _ => 0
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_str_non_nul(rec: *const Record) -> *const c_char
{
    match rec.as_ref() {
        Some(Record::String(s)) => s.as_ptr() as *const c_char,
        _ => null()
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_tuple(fields: *const *mut Record, len: size_t) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    Box::into_raw(Box::new(Record::Tuple(fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_tuple(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Tuple(_)) => true,
        _ => false
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_tuple_size(rec: *const Record) -> size_t {
    match rec.as_ref() {
        Some(Record::Tuple(recs)) => recs.len() as size_t,
        _ => 0
    }
}


#[no_mangle]
pub unsafe extern "C" fn ddlog_get_tuple_field(rec: *const Record, idx: size_t) -> *const Record {
    match rec.as_ref() {
        Some(Record::Tuple(recs)) => recs.get(idx).map(|r|r as *const Record).unwrap_or(null_mut()),
        _ => null_mut()
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
    match tup.as_mut() {
        Record::Tuple(recs) => recs.push(*rec),
        _ => {}
    };
    Box::into_raw(tup);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_vector(fields: *const *mut Record, len: size_t) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    Box::into_raw(Box::new(Record::Array(CollectionKind::Vector, fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_vector(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Vector,_)) => true,
        _ => false
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_vector_size(rec: *const Record) -> size_t {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Vector, recs)) => recs.len() as size_t,
        _ => 0
    }
}


#[no_mangle]
pub unsafe extern "C" fn ddlog_get_vector_elem(rec: *const Record, idx: size_t) -> *const Record {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Vector, recs)) => recs.get(idx).map(|r|r as *const Record).unwrap_or(null_mut()),
        _ => null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_vector_push(vec: *mut Record, rec: *mut Record) {
    let rec = Box::from_raw(rec);
    let mut vec = Box::from_raw(vec);
    match vec.as_mut() {
        Record::Array(CollectionKind::Vector, recs) => recs.push(*rec),
        _ => {}
    };
    Box::into_raw(vec);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_set(fields: *const *mut Record, len: size_t) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    Box::into_raw(Box::new(Record::Array(CollectionKind::Set, fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_set(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Set,_)) => true,
        _ => false
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_set_size(rec: *const Record) -> size_t {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Set, recs)) => recs.len() as size_t,
        _ => 0
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_set_elem(rec: *const Record, idx: size_t) -> *const Record {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Set, recs)) => recs.get(idx).map(|r|r as *const Record).unwrap_or(null_mut()),
        _ => null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_set_push(set: *mut Record, rec: *mut Record) {
    let rec = Box::from_raw(rec);
    let mut set = Box::from_raw(set);
    match set.as_mut() {
        Record::Array(CollectionKind::Set, recs) => recs.push(*rec),
        _ => {}
    };
    Box::into_raw(set);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_map(fields: *const *mut Record, len: size_t) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    Box::into_raw(Box::new(Record::Array(CollectionKind::Map, fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_map(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Map,_)) => true,
        _ => false
    }
}


#[no_mangle]
pub unsafe extern "C" fn ddlog_get_map_size(rec: *const Record) -> size_t {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Map, recs)) => recs.len() as size_t,
        _ => 0
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_map_key(rec: *const Record, idx: size_t) -> *const Record {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Map, recs)) => {
            recs.get(idx).map(|r| match r {
                Record::Tuple(kv) if kv.len() == 2 => kv.get(0).unwrap() as *const Record,
                _ => null_mut()
            }).unwrap_or(null_mut())
        },
        _ => null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_map_val(rec: *const Record, idx: size_t) -> *const Record {
    match rec.as_ref() {
        Some(Record::Array(CollectionKind::Map, recs)) => {
            recs.get(idx).map(|r| match r {
                Record::Tuple(kv) if kv.len() == 2 => kv.get(1).unwrap() as *const Record,
                _ => null_mut()
            }).unwrap_or(null_mut())
        },
        _ => null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_map_push(map: *mut Record, key: *mut Record, val: *mut Record) {
    let key = Box::from_raw(key);
    let val = Box::from_raw(val);
    let tup = Record::Tuple(vec![*key, *val]);
    let mut map = Box::from_raw(map);
    match map.as_mut() {
        Record::Array(CollectionKind::Map, recs) => recs.push(tup),
        _ => {}
    };
    Box::into_raw(map);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_struct(constructor: *const c_char, fields: *const *mut Record, len: size_t) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    let constructor = match CStr::from_ptr(constructor).to_str() {
        Ok(s) => s,
        Err(_) => {return null_mut();}
    };
    Box::into_raw(Box::new(Record::PosStruct(Cow::from(constructor.to_owned()), fields)))
}

/// Similar to `ddlog_struct()`, but expects `constructor` to be static string.
/// Doesn't allocate memory for a local copy of the string.
#[no_mangle]
pub unsafe extern "C" fn ddlog_struct_static_cons(constructor: *const c_char, fields: *const *mut Record, len: size_t) -> *mut Record {
    let fields = mk_record_vec(fields, len);
    let constructor = match CStr::from_ptr(constructor).to_str() {
        Ok(s) => s,
        Err(_) => {return null_mut();}
    };
    Box::into_raw(Box::new(Record::PosStruct(Cow::from(constructor), fields)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_is_struct(rec: *const Record) -> bool {
    match rec.as_ref() {
        Some(Record::PosStruct(_,_)) => true,
        Some(Record::NamedStruct(_,_)) => true,
        _ => false
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_struct_field(rec: *const Record, idx: size_t) -> *const Record {
    match rec.as_ref() {
        Some(Record::PosStruct(_, fields)) => fields.get(idx).map(|r|r as *const Record).unwrap_or(null_mut()),
        Some(Record::NamedStruct(_, fields)) => fields.get(idx).map(|(_, r)|r as *const Record).unwrap_or(null_mut()),
        _ => null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_constructor_non_null(rec: *const Record,
                                                        len: *mut size_t) -> *const c_char
{
    match rec.as_ref() {
        Some(Record::PosStruct(cons, _)) => {
            *len = cons.len();
            cons.as_ref().as_ptr() as *const c_char
        },
        Some(Record::NamedStruct(cons, _)) => {
            *len = cons.len();
            cons.as_ref().as_ptr() as *const c_char
        },
        _ => {
            *len = 0;
            null()
        }
    }
}

unsafe fn mk_record_vec(fields: *const *mut Record, len: size_t) -> Vec<Record> {
    let mut tfields = Vec::with_capacity(len as usize);
    for i in 0..len {
        tfields.push(*Box::from_raw(*fields.offset(i as isize)));
    };
    tfields
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_insert_cmd(table: libc::size_t, rec: *mut Record) -> *mut UpdCmd {
    let rec = Box::from_raw(rec);
    Box::into_raw(Box::new(UpdCmd::Insert(RelIdentifier::RelId(table), *rec)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delete_val_cmd(table: libc::size_t, rec: *mut Record) -> *mut UpdCmd {
    let rec = Box::from_raw(rec);
    Box::into_raw(Box::new(UpdCmd::Delete(RelIdentifier::RelId(table), *rec)))
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_delete_key_cmd(table: libc::size_t, rec: *mut Record) -> *mut UpdCmd {
    let rec = Box::from_raw(rec);
    Box::into_raw(Box::new(UpdCmd::DeleteKey(RelIdentifier::RelId(table), *rec)))
}

/// `Mutator` trait represents an object that can be used to mutate a value (e.g., change some of
/// its fields).
pub trait Mutator<V> {
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

impl FromRecord for u8 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_u8() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot convert {} to u8", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
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

#[test]
fn test_u8() {
    assert_eq!(u8::from_record(&Record::Int(25_u8.to_bigint().unwrap())), Ok(25));
    assert_eq!(u8::from_record(&Record::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u8::from_record(&Record::Int(0xabcd.to_bigint().unwrap())), Err("cannot convert 43981 to u8".to_string()));
    assert_eq!(u8::into_record(0x25), Record::Int(BigInt::from(0x25)));
}


impl FromRecord for u16 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_u16() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot convert {} to u16", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
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

#[test]
fn test_u16() {
    assert_eq!(u16::from_record(&Record::Int(25_u16.to_bigint().unwrap())), Ok(25));
    assert_eq!(u16::from_record(&Record::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u16::from_record(&Record::Int(0xabcdef.to_bigint().unwrap())), Err("cannot convert 11259375 to u16".to_string()));
    assert_eq!(u16::into_record(32000), Record::Int(BigInt::from(32000)));
}


impl FromRecord for u32 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_u32() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot convert {} to u32", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
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

#[test]
fn test_u32() {
    assert_eq!(u32::from_record(&Record::Int(25_u32.to_bigint().unwrap())), Ok(25));
    assert_eq!(u32::from_record(&Record::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u32::from_record(&Record::Int(0xabcdef.to_bigint().unwrap())), Ok(0xabcdef));
    assert_eq!(u32::from_record(&Record::Int((-0xabcdef).to_bigint().unwrap())), Err("cannot convert -11259375 to u32".to_string()));
}


impl FromRecord for u64 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_u64() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot convert {} to u64", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
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

#[test]
fn test_u64() {
    assert_eq!(u64::from_record(&Record::Int(25_u64.to_bigint().unwrap())), Ok(25));
    assert_eq!(u64::from_record(&Record::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u64::from_record(&Record::Int(0xabcdef.to_bigint().unwrap())), Ok(0xabcdef));
    assert_eq!(u64::from_record(&Record::Int((-0xabcdef).to_bigint().unwrap())), Err("cannot convert -11259375 to u64".to_string()));
}

impl FromRecord for u128 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_u128() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot convert {} to u128", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
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

#[test]
fn test_u128() {
    assert_eq!(u128::from_record(&Record::Int(25_u128.to_bigint().unwrap())), Ok(25));
    assert_eq!(u128::from_record(&Record::Int(100000000000000000000000000000000000000_u128.to_bigint().unwrap())), Ok(100000000000000000000000000000000000000));
    assert_eq!(u128::from_record(&Record::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u128::from_record(&Record::Int(0xabcdef.to_bigint().unwrap())), Ok(0xabcdef));
    assert_eq!(u128::from_record(&Record::Int((-0xabcdef).to_bigint().unwrap())), Err("cannot convert -11259375 to u128".to_string()));
}


impl FromRecord for BigInt {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => Result::Ok(i.clone()),
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
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

#[test]
fn test_bigint() {
    let v = (-25_i64).to_bigint().unwrap();
    assert_eq!(BigInt::from_record(&Record::Int(v.clone())), Ok(v));
}

impl FromRecord for BigUint {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_biguint() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot convert {} to BigUint", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
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

#[test]
fn test_biguint() {
    let vi = (25_i64).to_bigint().unwrap();
    let vu = (25_i64).to_biguint().unwrap();
    assert_eq!(BigUint::from_record(&Record::Int(vi)), Ok(vu));

    let vi = (-25_i64).to_bigint().unwrap();
    assert_eq!(BigUint::from_record(&Record::Int(vi)), Err("cannot convert -25 to BigUint".to_string()));
}


impl FromRecord for bool {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Bool(b) => { Result::Ok(*b) },
            v => {
                Result::Err(format!("not a bool {:?}", *v))
            }
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

#[test]
fn test_bool() {
    assert_eq!(bool::from_record(&Record::Bool(true)), Ok(true));
}


impl FromRecord for String {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::String(s) => { Result::Ok(s.clone()) },
            v => {
                Result::Err(format!("not a string {:?}", *v))
            }
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

#[test]
fn test_string() {
    assert_eq!(String::from_record(&Record::String("foo".to_string())), Ok("foo".to_string()));
    assert_eq!(String::from_record(&Record::Bool(true)), Err("not a string Bool(true)".to_string()));
}

impl FromRecord for () {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 0 => {
                Ok(())
            },
            v => { Result::Err(format!("not a 0-tuple {:?}", *v)) }
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
decl_tuple_from_record!(11, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10);
decl_tuple_from_record!(12, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11);
decl_tuple_from_record!(13, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11, T12, 12);
decl_tuple_from_record!(14, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11, T12, 12, T13, 13);
decl_tuple_from_record!(15, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11, T12, 12, T13, 13, T14, 14);
decl_tuple_from_record!(16, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11, T12, 12, T13, 13, T14, 14, T15, 15);
decl_tuple_from_record!(17, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11, T12, 12, T13, 13, T14, 14, T15, 15, T16, 16);
decl_tuple_from_record!(18, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11, T12, 12, T13, 13, T14, 14, T15, 15, T16, 16, T17, 17);
decl_tuple_from_record!(19, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11, T12, 12, T13, 13, T14, 14, T15, 15, T16, 16, T17, 17, T18, 18);
decl_tuple_from_record!(20, T0, 0, T1, 1, T2, 2, T3, 3, T4, 4, T5, 5, T6, 6, T7, 7, T8, 8, T9, 9, T10, 10, T11, 11, T12, 12, T13, 13, T14, 14, T15, 15, T16, 16, T17, 17, T18, 18, T19, 19);

#[test]
fn test_tuple() {
    assert_eq!(<(bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false)])),
               Ok((true, false)));
    assert_eq!(<(bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false)])),
               Ok((true, false, false)));
    assert_eq!(<(bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true)])),
               Ok((true, false, false, true)));
    assert_eq!(<(bool, bool, bool, bool, String)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string())])),
               Ok((true, false, false, true, "foo".to_string())));
    assert_eq!(<(bool, bool, bool, bool, String, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false)])),
               Ok((true, false, false, true, "foo".to_string(), false)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false)])),
               Ok((true, false, false, true, "foo".to_string(), false, false)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false)])),
               Ok((true, false, false, true, "foo".to_string(), false, false, false)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false), Record::Bool(true)])),
               Ok((true, false, false, true, "foo".to_string(), false, false, false, true)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::Bool(true)])),
               Ok((true, false, false, true, "foo".to_string(), false, false, false, true, true)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::Bool(true), Record::Bool(true)])),
               Ok((true, false, false, true, "foo".to_string(), false, false, false, true, true, true)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool, bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::Bool(true), Record::Bool(true), Record::Bool(false)])),
               Ok((true, false, false, true, "foo".to_string(), false, false, false, true, true, true, false)));
}


impl<T: FromRecord> FromRecord for vec::Vec<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Array(_,args) => {
                Result::from_iter(args.iter().map(|x| T::from_record(x)))
            },
            v => {
                T::from_record(v).map(|x|vec![x])
                //Result::Err(format!("not an array {:?}", *v))
            }
        }
    }
}

impl<T: IntoRecord> IntoRecord for vec::Vec<T> {
    fn into_record(self) -> Record {
        Record::Array(CollectionKind::Vector, self.into_iter().map(|x|x.into_record()).collect())
    }
}

impl<T: FromRecord> Mutator<vec::Vec<T>> for Record {
    fn mutate(&self, v: &mut vec::Vec<T>) -> Result<(), String> {
        *v = <vec::Vec<T>>::from_record(self)?;
        Ok(())
    }
}

#[test]
fn test_vec() {
    assert_eq!(<vec::Vec<bool>>::from_record(&Record::Array(CollectionKind::Unknown, vec![Record::Bool(true), Record::Bool(false)])),
               Ok(vec![true, false]));
}

macro_rules! decl_arr_from_record {
    ( $i:tt ) => {
        impl<T:FromRecord+Default> FromRecord for [T;$i] {
            fn from_record(val: &Record) -> Result<Self, String> {
                let vec = Vec::from_record(val)?;
                let mut arr = <[T;$i]>::default();
                if vec.len() != $i {
                    return Result::Err(format!("cannot convert {:?} to array of length {}", *val, $i))
                };
                let mut idx = 0;
                for v in vec.into_iter() {
                    arr[idx] = v;
                    idx = idx + 1;
                };
                Ok(arr)
            }
        }

        impl<T:IntoRecord+Clone> IntoRecord for [T;$i] {
            fn into_record(self) -> Record {
                Record::Array(CollectionKind::Vector, self.into_iter().map(|x:&T|(*x).clone().into_record()).collect())
            }
        }

        impl<T:FromRecord+Default> Mutator<[T;$i]> for Record {
            fn mutate(&self, v: &mut [T;$i]) -> Result<(), String> {
                *v = <[T;$i]>::from_record(self)?;
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

#[test]
fn test_array() {
    assert_eq!(<[bool;2]>::from_record(&Record::Array(CollectionKind::Vector, vec![Record::Bool(true), Record::Bool(false)])),
               Ok([true, false]));
    assert_eq!(<[bool;2]>::from_record(&Record::Array(CollectionKind::Vector, vec![Record::Bool(true)])),
               Err("cannot convert Array(Vector, [Bool(true)]) to array of length 2".to_owned()));
}


impl<K: FromRecord+Ord, V: FromRecord> FromRecord for BTreeMap<K,V> {
    fn from_record(val: &Record) -> Result<Self, String> {
        vec::Vec::from_record(val).map(|v|BTreeMap::from_iter(v))
    }
}

impl<K: IntoRecord+Ord, V:IntoRecord> IntoRecord for BTreeMap<K,V> {
    fn into_record(self) -> Record {
        Record::Array(CollectionKind::Map, self.into_iter().map(|x|x.into_record()).collect())
    }
}

/* Map update semantics is that the update contains keys that are in one of the maps but not the
 * other, plus keys that are in both maps but with different values. */
impl<K: FromRecord+Ord, V: FromRecord + PartialEq> Mutator<BTreeMap<K,V>> for Record {
    fn mutate(&self, map: &mut BTreeMap<K,V>) -> Result<(), String> {
        let upd = <BTreeMap<K,V>>::from_record(self)?;
        for (k, v) in upd.into_iter() {
            match map.entry(k) {
                btree_map::Entry::Vacant(ve) => {
                    /* key not in map -- insert */
                    ve.insert(v);
                },
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
        };
        Ok(())
    }
}

#[test]
fn test_map() {
    assert_eq!(<BTreeMap<u32, u32>>::from_record(&Record::Array(CollectionKind::Unknown,
                                                                vec![Record::Tuple(vec![Record::Int(BigInt::from(0)), Record::Int(BigInt::from(10))]),
                                                                     Record::Tuple(vec![Record::Int(BigInt::from(1)), Record::Int(BigInt::from(10))] ) ])),
               Ok(BTreeMap::from_iter(vec![(0,10), (1,10)])));

    let mut v = <BTreeMap<u32, u32>>::from_record(&Record::Array(CollectionKind::Unknown,
                                                  vec![Record::Tuple(vec![Record::Int(BigInt::from(0)), Record::Int(BigInt::from(10))]),
                                                       Record::Tuple(vec![Record::Int(BigInt::from(1)), Record::Int(BigInt::from(10))] ) ])).unwrap();
    Record::Array(CollectionKind::Unknown,
                  vec![Record::Tuple(vec![Record::Int(BigInt::from(0)), Record::Int(BigInt::from(10))]),
                       Record::Tuple(vec![Record::Int(BigInt::from(1)), Record::Int(BigInt::from(10))] ) ]).
               mutate(&mut v).unwrap();
    assert_eq!(v, BTreeMap::from_iter(vec![]));

    v = <BTreeMap<u32, u32>>::from_record(&Record::Array(CollectionKind::Unknown,
                                          vec![Record::Tuple(vec![Record::Int(BigInt::from(2)), Record::Int(BigInt::from(20))]),
                                               Record::Tuple(vec![Record::Int(BigInt::from(1)), Record::Int(BigInt::from(20))] ) ])).unwrap();
    Record::Array(CollectionKind::Unknown,
                             vec![Record::Tuple(vec![Record::Int(BigInt::from(0)), Record::Int(BigInt::from(10))]),
                                  Record::Tuple(vec![Record::Int(BigInt::from(1)), Record::Int(BigInt::from(10))] ) ]).
               mutate(&mut v).unwrap();
    assert_eq!(v, BTreeMap::from_iter(vec![(0,10), (1,10), (2,20)]));

    v =  <BTreeMap<u32, u32>>::from_record(&Record::Array(CollectionKind::Unknown,
                                            vec![Record::Tuple(vec![Record::Int(BigInt::from(2)), Record::Int(BigInt::from(20))]),
                                                 Record::Tuple(vec![Record::Int(BigInt::from(1)), Record::Int(BigInt::from(10))] ) ])).unwrap();
    Record::Array(CollectionKind::Unknown,
                             vec![Record::Tuple(vec![Record::Int(BigInt::from(0)), Record::Int(BigInt::from(10))]),
                                  Record::Tuple(vec![Record::Int(BigInt::from(1)), Record::Int(BigInt::from(10))] ) ]).
               mutate(&mut v).unwrap();
    assert_eq!(v, BTreeMap::from_iter(vec![(0,10), (2,20)]));
}

impl<T: FromRecord+Ord> FromRecord for BTreeSet<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        vec::Vec::from_record(val).map(|v|BTreeSet::from_iter(v))
    }
}

impl<T: IntoRecord+Ord> IntoRecord for BTreeSet<T> {
    fn into_record(self) -> Record {
        Record::Array(CollectionKind::Set, self.into_iter().map(|x|x.into_record()).collect())
    }
}

/* Set update semantics: update contains values that are in one of the maps but not the
 * other. */
impl<T: FromRecord+Ord> Mutator<BTreeSet<T>> for Record {
    fn mutate(&self, set: &mut BTreeSet<T>) -> Result<(), String> {
        let upd = <BTreeSet<T>>::from_record(self)?;
        for v in upd.into_iter() {
            if !set.remove(&v) {
                set.insert(v);
            }
        };
        Ok(())
    }
}

#[test]
fn test_set() {
    assert_eq!(<BTreeSet<u32>>::from_record(&Record::Array(CollectionKind::Unknown, vec![Record::Int(BigInt::from(0)), Record::Int(BigInt::from(1))])),
               Ok(BTreeSet::from_iter(vec![0, 1])));

    let mut v = <BTreeSet<u32>>::from_record(&Record::Array(CollectionKind::Unknown,
                                             vec![Record::Int(BigInt::from(0)), Record::Int(BigInt::from(2))])).unwrap();
    Record::Array(CollectionKind::Unknown, vec![Record::Int(BigInt::from(0)), Record::Int(BigInt::from(1))]).
               mutate(&mut v).unwrap();
    assert_eq!(v, BTreeSet::from_iter(vec![1, 2]));
}



#[cfg(test)]
#[derive(Eq, PartialEq, Debug, Clone, Default)]
struct Foo<T> {
    f1: T
}

pub fn arg_extract<T:FromRecord+Default>(args: &Vec<(Name, Record)>, argname: &str) -> Result<T, String> {
    args.iter().find(|(n,_)|*n==argname).map_or_else(||Ok(Default::default()), |(_,v)| T::from_record(v))
}

pub fn arg_find<'a>(args: &'a Vec<(Name, Record)>, argname: &str) -> Option<&'a Record> {
    args.iter().find(|(n,_)|*n==argname).map(|(_,v)|v)
}


#[cfg(test)]
impl <T: FromRecord+Default> FromRecord for Foo<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::PosStruct(constr, args) => {
                match constr.as_ref() {
                    "Foo" if args.len() == 1 => {
                        Ok(Foo{f1: T::from_record(&args[0])?})
                    },
                    c => Result::Err(format!("unknown constructor {} of type Foo in {:?}", c, *val))
                }
            },
            Record::NamedStruct(constr, args) => {
                match constr.as_ref() {
                    "Foo" => {
                        Ok(Foo{f1: arg_extract(args, "f1")?})
                    },
                    c => Result::Err(format!("unknown constructor {} of type Foo in {:?}", c, *val))
                }
            },
            v => {
                Result::Err(format!("not a struct {:?}", *v))
            }
        }
    }
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
            fn mutate(&self, x: &mut $n<$($targ),*>) -> Result<(), String> {
                match self {
                    $crate::record::Record::PosStruct(..) => {
                        return Err(format!("Cannot use positional struct as mutator"));
                    },
                    $crate::record::Record::NamedStruct(_, args) => {
                        $(if let Some(arg_upd) = $crate::record::arg_find(args, stringify!($arg)) {
                            <$crate::record::Mutator<$type>>::mutate(arg_upd, &mut x.$arg)?;
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

#[cfg(test)]
pub struct NestedStruct<T>{x:bool, y: Foo<T>}

#[cfg(test)]
pub struct StructWithNoFields;

#[cfg(test)]
decl_struct_into_record!(Foo, <T>, f1);

#[cfg(test)]
decl_record_mutator_struct!(Foo, <T>, f1: T);

#[cfg(test)]
decl_struct_into_record!(NestedStruct, <T>, x,y);

#[cfg(test)]
decl_record_mutator_struct!(NestedStruct, <T>, x: bool, y: Foo<T>);

#[cfg(test)]
decl_struct_into_record!(StructWithNoFields, <>,);

#[cfg(test)]
decl_record_mutator_struct!(StructWithNoFields, <>, );

#[test]
fn test_struct()
{
    let foo1: Foo<BTreeMap<u32, String>> = Foo{f1: BTreeMap::from_iter(vec![(5,"five".to_owned()), (6,"six".to_owned())])};
    let foo2 = <Foo<BTreeMap<u32, String>>>::from_record(&foo1.clone().into_record()).unwrap();

    assert_eq!(foo1, foo2);

    let upd = Record::NamedStruct(Cow::from("Foo"),
                                  vec![(Cow::from("f1"), Record::Array(CollectionKind::Unknown,
                                                                       vec![Record::Tuple(vec![Record::Int(BigInt::from(5)), Record::String("5".to_owned())])]))]);
    let mut foo_mod = foo1.clone();
    upd.mutate(&mut foo_mod).unwrap();
    let foo_expected = Foo{f1: BTreeMap::from_iter(vec![(5,"5".to_owned()), (6,"six".to_owned())])};
    assert_eq!(foo_mod, foo_expected);
}


#[cfg(test)]
type Bbool = bool;

#[cfg(test)]
#[derive(Eq, PartialEq, Debug, Clone)]
enum DummyEnum<T> {
    Constr1{f1: Bbool, f2: String},
    Constr2{f1: T, f2: BigInt, f3: Foo<T>},
    Constr3{f1: (bool, bool)},
}

#[cfg(test)]
impl <T: FromRecord+Default> FromRecord for DummyEnum<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::PosStruct(constr, args) => {
                match constr.as_ref() {
                    "Constr1" if args.len() == 2 => {
                        Ok(DummyEnum::Constr1{f1: <Bbool>::from_record(&args[0])?,
                                              f2: String::from_record(&args[1])?})
                    },
                    "Constr2" if args.len() == 3 => {
                        Ok(DummyEnum::Constr2{f1: <T>::from_record(&args[0])?,
                                              f2: <BigInt>::from_record(&args[1])?,
                                              f3: <Foo<T>>::from_record(&args[2])?})
                    },
                    "Constr3" if args.len() == 1 => {
                        Ok(DummyEnum::Constr3{f1: <(bool,bool)>::from_record(&args[0])?})
                    },
                    c => Result::Err(format!("unknown constructor {} of type DummyEnum in {:?}", c, *val))
                }
            },
            Record::NamedStruct(constr, args) => {
                match constr.as_ref() {
                    "Constr1" if args.len() == 2 => {
                        Ok(DummyEnum::Constr1{f1: arg_extract::<Bbool>(args, "f1")?,
                                              f2: arg_extract::<String>(args, "f2")?})
                    },
                    "Constr2" if args.len() == 3 => {
                        Ok(DummyEnum::Constr2{f1: arg_extract::<T>(args, "f1")?,
                                              f2: arg_extract::<BigInt>(args, "f2")?,
                                              f3: arg_extract::<Foo<T>>(args, "f3")?})
                    },
                    "Constr3" if args.len() == 1 => {
                        Ok(DummyEnum::Constr3{f1: arg_extract::<(bool,bool)>(args, "f1")?})
                    },
                    c => Result::Err(format!("unknown constructor {} of type DummyEnum in {:?}", c, *val))
                }
            },
            v => {
                Result::Err(format!("not a struct {:?}", *v))
            }
        }
    }
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
                                            <Mutator<$type>>::mutate(arg_upd, $arg)?;
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

#[cfg(test)]
decl_enum_into_record!(DummyEnum,<T>,Constr1{f1,f2},Constr2{f1,f2,f3},Constr3{f1});

#[cfg(test)]
decl_record_mutator_enum!(DummyEnum,<T>,Constr1{f1:Bbool ,f2: String},Constr2{f1: T, f2: BigInt, f3: Foo<T>},Constr3{f1: (bool, bool)});

#[test]
fn test_enum()
{
    assert_eq!(DummyEnum::from_record(&Record::PosStruct(Cow::from("Constr1"),
                                                    vec![Record::Bool(true), Record::String("foo".to_string())])),
               Ok(DummyEnum::Constr1::<bool>{f1: true, f2: "foo".to_string()}));
    assert_eq!(DummyEnum::from_record(&Record::NamedStruct(Cow::from("Constr1"),
                                                    vec![(Cow::from("f1"), Record::Bool(true)), (Cow::from("f2"), Record::String("foo".to_string()))])),
               Ok(DummyEnum::Constr1::<bool>{f1: true, f2: "foo".to_string()}));
    assert_eq!(DummyEnum::from_record(&Record::PosStruct(Cow::from("Constr2"),
                                                    vec![Record::Int((5_i64).to_bigint().unwrap()),
                                                         Record::Int((25_i64).to_bigint().unwrap()),
                                                         Record::PosStruct(Cow::from("Foo"), vec![Record::Int((0_i64).to_bigint().unwrap())])])),
               Ok(DummyEnum::Constr2::<u16>{f1: 5,
                                            f2: (25_i64).to_bigint().unwrap(),
                                            f3: Foo{f1: 0}}));
    assert_eq!(DummyEnum::from_record(&Record::NamedStruct(Cow::from("Constr2"),
                                                    vec![(Cow::from("f1"), Record::Int((5_i64).to_bigint().unwrap())),
                                                         (Cow::from("f2"), Record::Int((25_i64).to_bigint().unwrap())),
                                                         (Cow::from("f3"), Record::NamedStruct(Cow::from("Foo"),
                                                                                                vec![(Cow::from("f1"), Record::Int((0_i64).to_bigint().unwrap()))]))])),
               Ok(DummyEnum::Constr2::<u16>{f1: 5,
                                            f2: (25_i64).to_bigint().unwrap(),
                                            f3: Foo{f1: 0}}));

    let enm = DummyEnum::Constr2::<u16>{f1: 5,
                                        f2: (25_i64).to_bigint().unwrap(),
                                        f3: Foo{f1: 0}};
    assert_eq!(DummyEnum::from_record(&enm.clone().into_record()), Ok(enm));
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
