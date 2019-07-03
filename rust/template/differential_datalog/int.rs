#![allow(non_snake_case)]

use abomonation::Abomonation;
use num::bigint::{BigInt};
#[cfg(test)]
use num::bigint::{ToBigInt};
use std::str::FromStr;
use std::ops::*;
use serde::ser::*;
use serde::de::*;
use serde::de::Error;
use std::fmt;
use std::ffi::CStr;
use std::os::raw::c_char;
use super::record::{FromRecord, IntoRecord, Record, Mutator};
use super::uint;
use num::ToPrimitive;

#[derive(Eq, PartialOrd, PartialEq, Ord, Clone, Hash)]
pub struct Int{x:BigInt}

impl Default for Int {
    fn default() -> Int {
        Int{x: BigInt::default()}
    }
}
unsafe_abomonate!(Int);

impl Int {
    pub fn from_bigint(v: BigInt) -> Int {
        Int{x: v}
    }
    pub fn from_u8(v: u8) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_i8(v: i8) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_u16(v: u16) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_i16(v: i16) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_u32(v: u32) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_i32(v: i32) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_u64(v: u64) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_i64(v: i64) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_u128(v: u128) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_i128(v: i128) -> Int {
        Int{x: BigInt::from(v)}
    }
    pub fn from_Uint(v: uint::Uint) -> Int {
        v.to_Int().unwrap()
    }
    pub fn to_i8(&self) -> Option<i8> {
        self.x.to_i8()
    }
    pub fn to_i16(&self) -> Option<i16> {
        self.x.to_i16()
    }
    pub fn to_i32(&self) -> Option<i32> {
        self.x.to_i32()
    }
    pub fn to_i64(&self) -> Option<i64> {
        self.x.to_i64()
    }
    pub fn to_i128(&self) -> Option<i128> {
        self.x.to_i128()
    }
    pub fn to_Uint(&self) -> Option<uint::Uint> {
        self.x.to_biguint().map(|x| uint::Uint::from_biguint(x))
    }
    pub fn parse_bytes(buf: &[u8], radix: u32) -> Int {
        Int{x: BigInt::parse_bytes(buf, radix).unwrap()}
    }
}

#[no_mangle]
pub extern "C" fn int_from_i64(v: i64) -> *mut Int {
    Box::into_raw(Box::new(Int::from_i64(v)))
}

#[no_mangle]
pub extern "C" fn int_from_u64(v: u64) -> *mut Int {
    Box::into_raw(Box::new(Int::from_u64(v)))
}

#[no_mangle]
pub unsafe extern "C" fn int_from_str(s: *const c_char, radix: u32) -> *mut Int {
    let c_str = CStr::from_ptr(s);
    Box::into_raw(Box::new(Int::parse_bytes(c_str.to_bytes(), radix)))
}

#[no_mangle]
pub unsafe extern "C" fn int_free(x: *mut Int) {
    if x.is_null() {
        return;
    }
    Box::from_raw(x);
}


impl fmt::Display for Int {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.x)
    }
}

impl fmt::LowerHex for Int {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:x}", self.x)
    }
}

impl fmt::Debug for Int {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self, f)
    }
}


impl Serialize for Int {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.serialize_str(&self.x.to_str_radix(10))
    }
}

impl<'de> Deserialize<'de> for Int {
    fn deserialize<D>(deserializer: D) -> Result<Int, D::Error>
        where D: Deserializer<'de>
    {
        match String::deserialize(deserializer) {
            Ok(s) => match BigInt::from_str(&s) {
                        Ok(i)  => Ok(Int{x:i}),
                        Err(_) => Err(D::Error::custom(format!("invalid integer value: {}", s)))
                     },
            Err(e) => Err(e)
        }
    }
}


impl FromRecord for Int {
    fn from_record(val: &Record) -> Result<Self, String> {
        Ok(Int::from_bigint(BigInt::from_record(val)?))
    }
}

impl IntoRecord for Int {
    fn into_record(self) -> Record {
        self.x.into_record()
    }
}

impl Mutator<Int> for Record {
    fn mutate(&self, i: &mut Int) -> Result<(), String> {
        self.mutate(&mut i.x)
    }
}


#[test]
fn test_fromrecord() {
    let v = (-25_i64).to_bigint().unwrap();
    assert_eq!(Int::from_record(&Record::Int(v.clone())), Ok(Int::from_bigint(v)));
}

impl Shr<usize> for Int {
    type Output = Int;

    #[inline]
    fn shr(self, rhs: usize) -> Int {
        Int{x: self.x.shr(rhs)}
    }
}

impl Shl<usize> for Int {
    type Output = Int;

    #[inline]
    fn shl(self, rhs: usize) -> Int {
        Int{x: self.x.shl(rhs)}
    }
}

impl Neg for Int {
    type Output = Int;

    #[inline]
    fn neg(self) -> Self::Output {
        Int{x: self.x.neg()}
    }
}

macro_rules! forward_binop {
    (impl $imp:ident for $res:ty, $method:ident) => {
        impl $imp<$res> for $res {
            type Output = $res;

            #[inline]
            fn $method(self, other: $res) -> $res {
                // forward to val-ref
                Int{x: $imp::$method(self.x, other.x)}
            }
        }
    }
}

forward_binop!(impl Add for Int, add);
forward_binop!(impl Sub for Int, sub);
forward_binop!(impl Div for Int, div);
forward_binop!(impl Rem for Int, rem);
forward_binop!(impl Mul for Int, mul);

impl num::One for Int {
    fn one() -> Int {
        Int { x: BigInt::one() }
    }
}

impl num::Zero for Int {
    fn zero() -> Int {
        Int{ x: BigInt::zero() }
    }

    fn is_zero(&self) -> bool {
        self.x == BigInt::zero()
    }
}
