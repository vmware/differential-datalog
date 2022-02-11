use crate::{
    ddval::{DDVal, DDValMethods},
    record::{FromRecord, IntoRecord, Mutator, Record},
};
use abomonation::Abomonation;
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use std::{
    any::TypeId,
    cmp::Ordering,
    fmt::{self, Debug, Display, Formatter},
    hash::{Hash, Hasher},
};

/// DDValue: this type is stored in all DD collections.
/// It consists of value and associated vtable.
pub struct DDValue {
    pub(super) val: DDVal,
    pub(super) vtable: &'static DDValMethods,
}

impl Drop for DDValue {
    #[inline]
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(&mut self.val) }
    }
}

impl DDValue {
    #[inline]
    pub fn new(val: DDVal, vtable: &'static DDValMethods) -> DDValue {
        DDValue { val, vtable }
    }

    #[inline]
    pub fn into_ddval(self) -> DDVal {
        let res = DDVal { v: self.val.v };
        std::mem::forget(self);

        res
    }

    #[inline]
    pub fn type_id(&self) -> TypeId {
        (self.vtable.type_id)(&self.val)
    }

    #[inline]
    pub fn safe_cmp(&self, other: &DDValue) -> Ordering {
        match (self.vtable.type_id)(&self.val).cmp(&(other.vtable.type_id)(&other.val)) {
            Ordering::Equal => unsafe { (self.vtable.cmp)(&self.val, &other.val) },
            ord => ord,
        }
    }

    #[inline]
    pub fn safe_eq(&self, other: &DDValue) -> bool {
        ((self.vtable.type_id)(&self.val) == (other.vtable.type_id)(&other.val))
            && (unsafe { (self.vtable.eq)(&self.val, &other.val) })
    }
}

impl Mutator<DDValue> for Record {
    #[inline]
    fn mutate(&self, x: &mut DDValue) -> Result<(), String> {
        (x.vtable.mutate)(&mut x.val, self)
    }
}

impl IntoRecord for DDValue {
    fn into_record(self) -> Record {
        (self.vtable.into_record)(self.into_ddval())
    }
}

impl FromRecord for DDValue {
    fn from_record(_val: &Record) -> Result<Self, String> {
        Err("'DDValue::from_record': not implemented".to_string())
    }
}

impl Abomonation for DDValue {
    unsafe fn entomb<W: std::io::Write>(&self, _write: &mut W) -> std::io::Result<()> {
        panic!("'DDValue::entomb': not implemented")
    }

    unsafe fn exhume<'a, 'b>(&'a mut self, _bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        panic!("'DDValue::exhume': not implemented")
    }

    fn extent(&self) -> usize {
        panic!("'DDValue::extent': not implemented")
    }
}

/// `Serialize` implementation simply forwards the `serialize` operation to the
/// inner object.
impl Serialize for DDValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        erased_serde::serialize((self.vtable.ddval_serialize)(&self.val), serializer)
    }
}

/// Bogus `Deserialize` implementation, necessary to use `DDValue` type in DDlog
/// programs.  We cannot provide a proper `Deserialize` implementation for `DDValue`,
/// as there is no object to forward `deserialize` to.
impl<'de> Deserialize<'de> for DDValue {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(D::Error::custom("cannot deserialize 'DDValue' type"))
    }
}

impl Display for DDValue {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        (self.vtable.fmt_display)(&self.val, f)
    }
}

impl Debug for DDValue {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        (self.vtable.fmt_debug)(&self.val, f)
    }
}

impl PartialOrd for DDValue {
    fn partial_cmp(&self, other: &DDValue) -> Option<Ordering> {
        /* Safety: The types of both values are the same.
         * This should be enforced by the DDlog type checker. */
        debug_assert_eq!(
            (self.vtable.type_id)(&self.val),
            (other.vtable.type_id)(&other.val),
            "DDValue::partial_cmp: attempted to compare two values of different types"
        );
        unsafe { (self.vtable.partial_cmp)(&self.val, &other.val) }
    }
}

impl PartialEq for DDValue {
    fn eq(&self, other: &Self) -> bool {
        /* Safety: The types of both values are the same.
         * This should be enforced by the DDlog type checker. */
        debug_assert_eq!(
            (self.vtable.type_id)(&self.val),
            (other.vtable.type_id)(&other.val),
            "DDValue::eq: attempted to compare two values of different types"
        );
        unsafe { (self.vtable.eq)(&self.val, &other.val) }
    }
}

impl Eq for DDValue {}

impl Ord for DDValue {
    fn cmp(&self, other: &Self) -> Ordering {
        /* Safety: The types of both values are the same.
         * This should be enforced by the DDlog type checker. */
        debug_assert_eq!(
            (self.vtable.type_id)(&self.val),
            (other.vtable.type_id)(&other.val),
            "DDValue::cmp: attempted to compare two values of different types"
        );
        unsafe { (self.vtable.cmp)(&self.val, &other.val) }
    }
}

impl Clone for DDValue {
    fn clone(&self) -> Self {
        DDValue {
            val: (self.vtable.clone)(&self.val),
            vtable: self.vtable,
        }
    }
}

impl Hash for DDValue {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        (self.vtable.hash)(&self.val, state)
    }
}
