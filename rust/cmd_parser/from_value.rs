//! `FromValue` trait.  For types that 

use parse::Value;
use num::{ToPrimitive, BigInt, BigUint};

#[cfg(test)]
use num::bigint::{ToBigInt, ToBigUint};

pub trait FromValue: Sized {
    fn from_value(val: &Value) -> Result<Self, String>;
}

impl FromValue for u8 {
    fn from_value(val: &Value) -> Result<Self, String> {
        match val {
            Value::Int(i) => {
                match i.to_u8() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to u8", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v).to_string())
            }
        }
    }
}


#[test]
fn test_u8() {
    assert_eq!(u8::from_value(&Value::Int(25_u8.to_bigint().unwrap())), Ok(25));
    assert_eq!(u8::from_value(&Value::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u8::from_value(&Value::Int(0xabcd.to_bigint().unwrap())), Err("cannot conver 43981 to u8".to_string()));
}


impl FromValue for u16 {
    fn from_value(val: &Value) -> Result<Self, String> {
        match val {
            Value::Int(i) => {
                match i.to_u16() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to u16", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v).to_string())
            }
        }
    }
}

#[test]
fn test_u16() {
    assert_eq!(u16::from_value(&Value::Int(25_u16.to_bigint().unwrap())), Ok(25));
    assert_eq!(u16::from_value(&Value::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u16::from_value(&Value::Int(0xabcdef.to_bigint().unwrap())), Err("cannot conver 11259375 to u16".to_string()));
}


impl FromValue for u32 {
    fn from_value(val: &Value) -> Result<Self, String> {
        match val {
            Value::Int(i) => {
                match i.to_u32() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to u32", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v).to_string())
            }
        }
    }
}

#[test]
fn test_u32() {
    assert_eq!(u32::from_value(&Value::Int(25_u32.to_bigint().unwrap())), Ok(25));
    assert_eq!(u32::from_value(&Value::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u32::from_value(&Value::Int(0xabcdef.to_bigint().unwrap())), Ok(0xabcdef));
    assert_eq!(u32::from_value(&Value::Int((-0xabcdef).to_bigint().unwrap())), Err("cannot conver -11259375 to u32".to_string()));
}


impl FromValue for u64 {
    fn from_value(val: &Value) -> Result<Self, String> {
        match val {
            Value::Int(i) => {
                match i.to_u64() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to u64", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v).to_string())
            }
        }
    }
}

#[test]
fn test_u64() {
    assert_eq!(u64::from_value(&Value::Int(25_u64.to_bigint().unwrap())), Ok(25));
    assert_eq!(u64::from_value(&Value::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u64::from_value(&Value::Int(0xabcdef.to_bigint().unwrap())), Ok(0xabcdef));
    assert_eq!(u64::from_value(&Value::Int((-0xabcdef).to_bigint().unwrap())), Err("cannot conver -11259375 to u64".to_string()));
}

impl FromValue for BigInt {
    fn from_value(val: &Value) -> Result<Self, String> {
        match val {
            Value::Int(i) => Result::Ok(i.clone()),
            v => {
                Result::Err(format!("not an int {:?}", *v).to_string())
            }
        }
    }
}

#[test]
fn test_bigint() {
    let v = (-25_i64).to_bigint().unwrap();
    assert_eq!(BigInt::from_value(&Value::Int(v.clone())), Ok(v));
}


impl FromValue for BigUint {
    fn from_value(val: &Value) -> Result<Self, String> {
        match val {
            Value::Int(i) => {
                match i.to_biguint() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to BigUint", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v).to_string())
            }
        }
    }
}

#[test]
fn test_biguint() {
    let vi = (25_i64).to_bigint().unwrap();
    let vu = (25_i64).to_biguint().unwrap();
    assert_eq!(BigUint::from_value(&Value::Int(vi)), Ok(vu));

    let vi = (-25_i64).to_bigint().unwrap();
    assert_eq!(BigUint::from_value(&Value::Int(vi)), Err("cannot conver -25 to BigUint".to_string()));
}


impl FromValue for bool {
    fn from_value(val: &Value) -> Result<Self, String> {
        match val {
            Value::Bool(b) => { Result::Ok(*b) },
            v => {
                Result::Err(format!("not a bool {:?}", *v).to_string())
            }
        }
    }
}

#[test]
fn test_bool() {
    assert_eq!(bool::from_value(&Value::Bool(true)), Ok(true));
}


impl FromValue for String {
    fn from_value(val: &Value) -> Result<Self, String> {
        match val {
            Value::String(s) => { Result::Ok(s.clone()) },
            v => {
                Result::Err(format!("not a string {:?}", *v).to_string())
            }
        }
    }
}

#[test]
fn test_string() {
    assert_eq!(String::from_value(&Value::String("foo".to_string())), Ok("foo".to_string()));
    assert_eq!(String::from_value(&Value::Bool(true)), Err("not a string Bool(true)".to_string()));
}
