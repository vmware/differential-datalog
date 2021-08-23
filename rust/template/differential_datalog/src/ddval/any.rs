//! `Any` is a wrapper around `DDValue` that makes it safe to use
//! outside of the DDlog runtime by providing safe implementations
//! of `Ord` ans `Eq` traits, which work even if their arguments
//! wrap different types.

use crate::{
    ddval::DDValue,
    program::RelId,
    record::{FromRecord, IntoRecord, Mutator, Record},
    AnyDeserialize, AnyDeserializeFunc,
};

use std::{
    cmp::Ordering,
    fmt,
    fmt::{Debug, Formatter},
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    option::Option,
    result::Result,
};

use serde::{
    de::{DeserializeSeed, Error},
    Deserialize, Deserializer, Serialize, Serializer,
};

#[derive(Default, Clone)]
pub struct Any(DDValue);

impl Hash for Any {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.deref().hash(state)
    }
}

impl Any {
    pub fn new(val: DDValue) -> Self {
        Any(val)
    }
}

impl From<DDValue> for Any {
    fn from(val: DDValue) -> Self {
        Any::new(val)
    }
}

impl From<Any> for DDValue {
    fn from(any: Any) -> Self {
        any.0
    }
}

impl Deref for Any {
    type Target = DDValue;

    fn deref(&self) -> &DDValue {
        &self.0
    }
}

impl DerefMut for Any {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Debug for Any {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Debug::fmt(self.deref(), f)
    }
}

impl IntoRecord for Any {
    fn into_record(self) -> Record {
        self.0.into_record()
    }
}

impl FromRecord for Any {
    fn from_record(val: &Record) -> Result<Self, String> {
        DDValue::from_record(val).map(Self::new)
    }
}

impl Mutator<Any> for Record {
    fn mutate(&self, v: &mut Any) -> Result<(), String> {
        self.mutate(v.deref_mut())
    }
}

impl Eq for Any {}

impl PartialEq for Any {
    fn eq(&self, other: &Self) -> bool {
        self.safe_eq(other)
    }
}

impl PartialOrd for Any {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.safe_cmp(&*other))
    }
}

impl Ord for Any {
    fn cmp(&self, other: &Self) -> Ordering {
        self.safe_cmp(&*other)
    }
}

impl Serialize for Any {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.deref().serialize(serializer)
    }
}

// This implementation always fails, as we cannot deserialize `Any`
// without knowing the actual type.
impl<'de> Deserialize<'de> for Any {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        DDValue::deserialize(deserializer).map(Self::new)
    }
}

/// A `DeserializeSeed` implementation for `Any` that wraps an instance
/// of `AnyDeserializeFunc`.
/// This can be used to construct `Deserialize` implementations for more
/// complex types that contain fields of type `Any`.
pub struct AnyDeserializeSeed(AnyDeserializeFunc);

impl AnyDeserializeSeed {
    pub fn new(func: AnyDeserializeFunc) -> Self {
        AnyDeserializeSeed(func)
    }

    pub fn from_relid<D>(ddlog: &D, relid: RelId) -> Option<Self>
    where
        D: AnyDeserialize + ?Sized,
    {
        ddlog.get_deserialize(relid).map(AnyDeserializeSeed::new)
    }
}

impl<'de> DeserializeSeed<'de> for AnyDeserializeSeed {
    type Value = Any;

    fn deserialize<D>(self, deserializer: D) -> Result<Any, D::Error>
    where
        D: Deserializer<'de>,
    {
        (self.0)(&mut <dyn erased_serde::Deserializer>::erase(deserializer))
            .map_err(D::Error::custom)
    }
}
