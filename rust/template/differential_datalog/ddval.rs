use std::any::Any;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use serde::de::Deserialize;
use serde::de::Deserializer;
use serde::ser::Serialize;
use serde::ser::Serializer;

use abomonation::Abomonation;

use crate::record::IntoRecord;
use crate::record::Mutator;
use crate::record::Record;

/// Trait for values that can be stored in DDlog relations.
#[typetag::serde]
pub trait DDVal: DDValMethods {
    fn val_clone(&self) -> Arc<dyn DDVal>;
}

pub trait DDValMethods: Send + Debug + Display + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + 'static + Send + Sync>;
    fn val_into_record(self: Arc<Self>) -> Record;
    fn val_eq(&self, other: &dyn DDVal) -> bool;
    fn val_partial_cmp(&self, other: &dyn DDVal) -> Option<std::cmp::Ordering>;
    fn val_cmp(&self, other: &dyn DDVal) -> std::cmp::Ordering;
    fn val_hash(&self, state: &mut dyn Hasher);
    fn val_mutate(&mut self, record: &Record) -> Result<(), String>;
}

impl<T> DDValMethods for T
where
    T: Eq + Ord + Clone + Send + Debug + Display + Sync + Hash + PartialOrd + IntoRecord + 'static,
    Record: Mutator<T>,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + 'static + Send + Sync> {
        self
    }
    fn val_eq(&self, other: &dyn DDVal) -> bool {
        self.eq(other.as_any().downcast_ref::<T>().unwrap())
    }
    fn val_partial_cmp(&self, other: &dyn DDVal) -> Option<std::cmp::Ordering> {
        self.partial_cmp(other.as_any().downcast_ref::<T>().unwrap())
    }
    fn val_cmp(&self, other: &dyn DDVal) -> std::cmp::Ordering {
        self.cmp(other.as_any().downcast_ref::<T>().unwrap())
    }
    fn val_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state)
    }
    fn val_into_record(self: Arc<Self>) -> Record {
        (*self).clone().into_record()
    }
    fn val_mutate(&mut self, record: &Record) -> Result<(), String> {
        record.mutate(self)
    }
}

#[derive(Debug)]
pub struct DDValue {
    val: Arc<dyn DDVal>,
}

impl DDValue {
    pub fn new(val: Arc<dyn DDVal>) -> DDValue {
        DDValue { val }
    }

    pub fn val(&self) -> &dyn DDVal {
        &(*self.val)
    }

    pub fn mut_val(&mut self) -> &mut dyn DDVal {
        // The borrow checker does not allow the following optimization.
        /*if let Some(v) = Arc::get_mut(&mut self.val) {
            return v;
        };*/

        self.val = (*self.val).val_clone();
        Arc::get_mut(&mut self.val).unwrap()
    }

    pub fn into_val(self) -> Arc<dyn DDVal> {
        self.val
    }
}

impl Mutator<DDValue> for Record {
    fn mutate(&self, x: &mut DDValue) -> Result<(), String> {
        x.mut_val().val_mutate(self)
    }
}

impl IntoRecord for DDValue {
    fn into_record(self) -> Record {
        self.val.val_into_record()
    }
}

impl Abomonation for DDValue {
    unsafe fn entomb<W: std::io::Write>(&self, _write: &mut W) -> std::io::Result<()> {
        panic!("DDValue::entomb: not implemented")
    }
    unsafe fn exhume<'a, 'b>(&'a mut self, _bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        panic!("DDValue::exhume: not implemented")
    }
    fn extent(&self) -> usize {
        panic!("DDValue::extent: not implemented")
    }
}

impl Serialize for DDValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.val.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DDValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val: Box<dyn DDVal> = Deserialize::deserialize(deserializer)?;
        Ok(DDValue {
            val: From::from(val),
        })
    }
}

impl Display for DDValue {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        Display::fmt(&self.val, f)
    }
}
impl PartialOrd for DDValue {
    fn partial_cmp(&self, other: &DDValue) -> Option<std::cmp::Ordering> {
        self.val.val_partial_cmp(&*other.val)
    }
}

impl PartialEq for DDValue {
    fn eq(&self, other: &Self) -> bool {
        self.val.val_eq(&*other.val)
    }
}

impl Eq for DDValue {}

impl Ord for DDValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.val.val_cmp(&*other.val)
    }
}

impl Clone for DDValue {
    fn clone(&self) -> Self {
        DDValue {
            val: self.val.clone(),
        }
    }
}

impl Hash for DDValue {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.val.val_hash(state)
    }
}

/// Trait to convert `DDValue` into concrete value types and back.
pub trait DDValConvert {
    /// Extract reference to concrete type from `DDValue`.  Panics if `v` does not contain a
    /// value of type `Self`.
    fn from_ddvalue_ref(v: &DDValue) -> &Self;

    /// Extracts the concrete value contained in `v`.  Panics if `v` does not contain a value of
    /// type `Self`.
    fn from_ddvalue(v: DDValue) -> Arc<Self>;

    /// Extract reference to concrete type from `&dyn DDVal`.  Panics if `v` does not contain a
    /// value of type `Self`.
    fn from_ddval_ref(v: &dyn DDVal) -> &Self;

    /// Extracts the concrete value contained in `v`.  Panics if `v` does not contain a value of
    /// type `Self`.
    fn from_ddval(v: Arc<dyn DDVal>) -> Arc<Self>;

    /// Wrap a value in a `DDValue`, erasing its original type.  This is a safe conversion that
    /// cannot fail.
    fn into_ddvalue(self) -> DDValue;
}

impl<T> DDValConvert for T
where
    T: DDVal,
{
    fn from_ddvalue_ref(v: &DDValue) -> &T {
        Self::from_ddval_ref(v.val())
    }
    fn from_ddvalue(v: DDValue) -> Arc<T> {
        Self::from_ddval(v.into_val())
    }
    fn from_ddval_ref(v: &dyn DDVal) -> &T {
        v.as_any().downcast_ref::<T>().unwrap()
    }
    fn from_ddval(v: Arc<dyn DDVal>) -> Arc<T> {
        v.into_any().downcast::<T>().unwrap()
    }
    fn into_ddvalue(self) -> DDValue {
        DDValue::new(Arc::new(self))
    }
}
