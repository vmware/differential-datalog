use abomonation::Abomonation;
use num::bigint::{BigUint, BigInt, ToBigUint, ToBigInt};
use std::str::FromStr;
use std::ops::*;
use serde::ser::*;
use serde::de::*;
use serde::de::Error;
use std::fmt;
use cmd_parser::{FromRecord, Record};

#[derive(Eq, PartialOrd, PartialEq, Ord, Debug, Clone, Hash)]
pub struct Uint{x:BigUint}

impl Default for Uint {
    fn default() -> Uint {
        Uint{x: BigUint::default()}
    }
}
unsafe_abomonate!(Uint);

impl Uint {
    pub fn from_biguint(v: BigUint) -> Uint {
        Uint{x: v}
    }
    pub fn from_bigint(v: BigInt) -> Uint {
        Uint{x: v.to_biguint().unwrap()}
    }
    pub fn from_u64(v: u64) -> Uint {
        Uint{x: BigUint::from(v)}
    }
    pub fn parse_bytes(buf: &[u8], radix: u32) -> Uint {
        Uint{x: BigUint::parse_bytes(buf, radix).unwrap()}
    }
}

impl fmt::Display for Uint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.x)
    }
}

impl Serialize for Uint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.serialize_str(&self.x.to_str_radix(10))
    }
}

impl<'de> Deserialize<'de> for Uint {
    fn deserialize<D>(deserializer: D) -> Result<Uint, D::Error>
        where D: Deserializer<'de>
    {
        match String::deserialize(deserializer) {
            Ok(s) => match BigUint::from_str(&s) {
                        Ok(i)  => Ok(Uint{x:i}),
                        Err(_) => Err(D::Error::custom(format!("invalid integer value: {}", s)))
                     },
            Err(e) => Err(e)
        }
    }
}

impl FromRecord for Uint {
    fn from_record(val: &Record) -> Result<Self, String> {
        Ok(Uint::from_biguint(BigUint::from_record(val)?))
    }
}

#[test]
fn test_fromrecord() {
    let v = (25_u64).to_bigint().unwrap();
    assert_eq!(Uint::from_record(&Record::Int(v.clone())), Ok(Uint::from_bigint(v)));
}

/*
impl Uint {
    #[inline]
    pub fn parse_bytes(buf: &[u8], radix: u32) -> Uint {
        Uint{x: BigUint::parse_bytes(buf, radix).unwrap()}
    }
}
*/

impl Shr<usize> for Uint {
    type Output = Uint;

    #[inline]
    fn shr(self, rhs: usize) -> Uint {
        Uint{x: self.x.shr(rhs)}
    }
}

impl Shl<usize> for Uint {
    type Output = Uint;

    #[inline]
    fn shl(self, rhs: usize) -> Uint {
        Uint{x: self.x.shl(rhs)}
    }
}

macro_rules! forward_binop {
    (impl $imp:ident for $res:ty, $method:ident) => {
        impl $imp<$res> for $res {
            type Output = $res;

            #[inline]
            fn $method(self, other: $res) -> $res {
                // forward to val-ref
                Uint{x: $imp::$method(self.x, other.x)}
            }
        }
    }
}

forward_binop!(impl Add for Uint, add);
forward_binop!(impl Sub for Uint, sub);
forward_binop!(impl Div for Uint, div);
forward_binop!(impl Rem for Uint, rem);
forward_binop!(impl BitAnd for Uint, bitand);
forward_binop!(impl BitOr for Uint, bitor);
