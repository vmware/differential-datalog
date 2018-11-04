use serde::ser::*;
use serde::de::*;
use std::sync::Arc;
use std::fmt;
use std::io;
use abomonation::Abomonation;
use program::Val;
use std::ops::Deref;
use super::record::{FromRecord, IntoRecord, Record};


#[derive(Eq, PartialOrd, PartialEq, Ord, Clone, Hash)]
pub struct ArcVal<T: Val>{x: Arc<T>}

impl<T: Val+Default> Default for ArcVal<T> {
    fn default() -> Self {
        Self{x: Arc::new(T::default())}
    }
}

impl<T: Val> Deref for ArcVal<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*self.x
    }
}

impl<T:Val> From<T> for ArcVal<T> {
    fn from(x: T) -> Self {
        Self{x: Arc::new(x)}
    }
}

impl<T:Val> Abomonation for ArcVal<T> {
    unsafe fn entomb<W: io::Write>(&self, write: &mut W) -> io::Result<()> {
        self.deref().entomb(write)
    }
    unsafe fn exhume<'a, 'b>(
        &'a mut self, 
        bytes: &'b mut [u8]
    ) -> Option<&'b mut [u8]> {
        Arc::get_mut(&mut self.x).unwrap().exhume(bytes)
    }
    fn extent(&self) -> usize {
        self.deref().extent()
    }
}

pub type DDString = ArcVal<String>;


impl ArcVal<String> {
    pub fn from_str(s: &str) -> Self {
        Self{x: Arc::new(s.to_string())}
    }
    /*pub fn string(&self) -> &String {
        &*self.x
    }*/
    pub fn str(&self) -> &str {
        &self.deref().as_str()
    }
    pub fn concat(&self, x: &str) -> Self{
        Self::from(self.deref().clone() + x)
    }
}


impl<T: Val> fmt::Display for ArcVal<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.deref().fmt(f)
    }
}

impl<T: Val> fmt::Debug for ArcVal<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.deref().fmt(f)
    }
}

impl<T: Val> Serialize for ArcVal<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        self.deref().serialize(serializer)
    }
}

impl<'de, T: Val> Deserialize<'de> for ArcVal<T> {
    fn deserialize<D>(deserializer: D) -> Result<ArcVal<T>, D::Error>
        where D: Deserializer<'de>
    {
        T::deserialize(deserializer).map(|x|Self::from(x))
    }
}

impl<T: Val+FromRecord> FromRecord for ArcVal<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        T::from_record(val).map(|x|Self::from(x))
    }
}

impl<T: Val+IntoRecord> IntoRecord for ArcVal<T> {
    fn into_record(self) -> Record {
        (*self.x).clone().into_record()
    }
}
