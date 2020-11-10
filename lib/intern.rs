use differential_datalog::record;
use differential_datalog::record::*;
use once_cell::sync::Lazy;
use serde;
use std::collections;
use std::fmt;
use std::hash::Hash;
use std::marker;
use std::sync;
use std::vec;

type Symbol = u32;

#[derive(Eq, PartialOrd, PartialEq, Ord, Clone, Hash)]
pub struct IObj<T> {
    pub x: Symbol,
    phantom: marker::PhantomData<T>,
}

struct Interner<T> {
    vec: vec::Vec<T>,
    map: collections::HashMap<T, Symbol>,
}

impl<T: Clone + Eq + Hash> Interner<T> {
    pub fn new() -> Self {
        Interner {
            vec: vec::Vec::new(),
            map: collections::HashMap::new(),
        }
    }
    pub fn intern(&mut self, x: &T) -> Symbol {
        if !self.map.contains_key(x) {
            let len = self.map.len() as Symbol;
            self.vec.push(x.clone());
            self.map.insert(x.clone(), len);
            len
        } else {
            *self.map.get(x).unwrap()
        }
    }
    pub fn get(&self, sym: Symbol) -> &T {
        &self.vec[sym as usize]
    }
}

type StringInterner = Interner<String>;

// TODO: Arc is unneeded
static global_string_interner: Lazy<sync::Arc<sync::Mutex<StringInterner>>> =
    Lazy::new(|| sync::Arc::new(sync::Mutex::new(StringInterner::new())));

// TODO: Arc can be replaced by an `&'static Mutex<_>`
thread_local!(static STRING_INTERNER: sync::Arc<sync::Mutex<StringInterner>> = global_string_interner.clone());

impl Default for IString {
    fn default() -> Self {
        string_intern(&String::default())
    }
}

pub fn string_intern(s: &String) -> IString {
    STRING_INTERNER.with(|si| IObj {
        x: si.lock().unwrap().intern(s),
        phantom: Default::default(),
    })
}

pub fn istring_str(s: &IString) -> String {
    STRING_INTERNER.with(|si| si.lock().unwrap().get(s.x).clone())
}

pub fn istring_ord(s: &IString) -> u32 {
    s.x
}

impl fmt::Display for IString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        record::format_ddlog_str(&istring_str(self), f)
    }
}

impl fmt::Debug for IString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        record::format_ddlog_str(&istring_str(self), f)
    }
}

impl serde::Serialize for IString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        istring_str(self).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for IString {
    fn deserialize<D>(deserializer: D) -> Result<IString, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|s| string_intern(&s))
    }
}

impl FromRecord for IString {
    fn from_record(val: &Record) -> Result<Self, String> {
        String::from_record(val).map(|s| string_intern(&s))
    }
}

impl IntoRecord for IString {
    fn into_record(self) -> Record {
        istring_str(&self).into_record()
    }
}

impl Mutator<IString> for Record {
    fn mutate(&self, s: &mut IString) -> Result<(), String> {
        let mut string = istring_str(s);
        self.mutate(&mut string)?;
        *s = string_intern(&string);
        Ok(())
    }
}
