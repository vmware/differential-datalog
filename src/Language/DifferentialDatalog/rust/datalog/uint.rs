use abomonation::Abomonation;
use num::bigint::BigUint;
use std::str::FromStr;
use std::ops::*;
use serde::ser::*;
use serde::de::*;
use serde::de::Error;


#[derive(Eq, PartialOrd, PartialEq, Ord, Debug, Clone, Hash)]
pub struct Uint{x:BigUint}

impl Default for Uint {
    fn default() -> Uint {Uint{x: BigUint::default()}}
}
unsafe_abomonate!(Uint);

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
