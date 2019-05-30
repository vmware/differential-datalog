extern crate lazy_static;

pub use self::lazy_static::lazy_static;
use std::marker;
use std::thread;
use std::vec;
use std::collections;
use std::sync;
use serde;
use differential_datalog::record::Record;
use differential_datalog::record;

type Symbol = u32;

#[derive(Eq, PartialOrd, PartialEq, Ord, Clone, Hash)]
pub struct intern_IObj<T> {
    pub x: Symbol,
    phantom: marker::PhantomData<T>
}

struct Interner<T> {
    vec: vec::Vec<T>,
    map: collections::HashMap<T, Symbol>
}

impl <T: Clone + Eq + Hash> Interner<T> {
    pub fn new() -> Self { Interner { vec: vec::Vec::new(), map: collections::HashMap::new() } }
    pub fn intern(&mut self, x: &T) -> Symbol {
        if !self.map.contains_key(x) {
            let len = self.map.len() as Symbol;
            self.vec.push(x.clone());
            self.map.insert(x.clone(), len);
            len
        }
        else {
            *self.map.get(x).unwrap()
        }
    }
    pub fn get(&self, sym: Symbol) -> &T {
        &self.vec[sym as usize]
    }
}

type StringInterner = Interner<String>;

lazy_static! {
    static ref global_string_interner: sync::Arc<sync::Mutex<StringInterner>> = sync::Arc::new(sync::Mutex::new(StringInterner::new()));
}

thread_local!(static STRING_INTERNER: sync::Arc<sync::Mutex<StringInterner>> = global_string_interner.clone());

impl Default for intern_IString {
    fn default() -> Self {
        intern_string_intern(&String::default())
    }
}

pub fn intern_string_intern(s: &String) -> intern_IString {
    STRING_INTERNER.with(|si| intern_IObj{x: si.lock().unwrap().intern(s), phantom: Default::default()})
}

pub fn intern_istring_str(s: &intern_IString) -> String {
    STRING_INTERNER.with(|si| si.lock().unwrap().get(s.x).clone())
}

pub fn intern_istring_ord(s: &intern_IString) -> u32 {
    s.x
}



impl fmt::Display for intern_IString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        record::format_ddlog_str(&intern_istring_str(self), f)
    }
}


impl fmt::Debug for intern_IString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        record::format_ddlog_str(&intern_istring_str(self), f)
    }
}

impl serde::Serialize for intern_IString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer
    {
        intern_istring_str(self).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for intern_IString {
    fn deserialize<D>(deserializer: D) -> Result<intern_IString, D::Error>
        where D: serde::Deserializer<'de>
    {
        String::deserialize(deserializer).map(|s|intern_string_intern(&s))
    }
}


impl FromRecord for intern_IString {
    fn from_record(val: &Record) -> Result<Self, String> {
        String::from_record(val).map(|s|intern_string_intern(&s))
    }
}

impl IntoRecord for intern_IString {
    fn into_record(self) -> Record {
        intern_istring_str(&self).into_record()
    }
}

impl Mutator<intern_IString> for Record
{
    fn mutate(&self, s: &mut intern_IString) -> Result<(), String> {
        let mut string = intern_istring_str(s);
        self.mutate(&mut string)?;
        *s = intern_string_intern(&string);
        Ok(())
    }
}
