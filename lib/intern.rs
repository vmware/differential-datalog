use differential_datalog::record;
use differential_datalog::record::*;
use lazy_static::lazy_static;
use serde;
use std::collections;
use std::marker;
use std::sync;
use std::vec;

#[cfg(feature = "flatbuf")]
use flatbuf::{FromFlatBuffer, ToFlatBuffer, ToFlatBufferTable, ToFlatBufferVectorElement};

/* `flatc`-generated declarations re-exported by `flatbuf.rs` */
#[cfg(feature = "flatbuf")]
use flatbuf::fb;

/* FlatBuffers runtime */
#[cfg(feature = "flatbuf")]
use flatbuffers as fbrt;

type Symbol = u32;

#[derive(Eq, PartialOrd, PartialEq, Ord, Clone, Hash)]
pub struct intern_IObj<T> {
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

lazy_static! {
    static ref global_string_interner: sync::Arc<sync::Mutex<StringInterner>> =
        sync::Arc::new(sync::Mutex::new(StringInterner::new()));
}

thread_local!(static STRING_INTERNER: sync::Arc<sync::Mutex<StringInterner>> = global_string_interner.clone());

impl Default for intern_IString {
    fn default() -> Self {
        intern_string_intern(&String::default())
    }
}

pub fn intern_string_intern(s: &String) -> intern_IString {
    STRING_INTERNER.with(|si| intern_IObj {
        x: si.lock().unwrap().intern(s),
        phantom: Default::default(),
    })
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
    where
        S: serde::Serializer,
    {
        intern_istring_str(self).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for intern_IString {
    fn deserialize<D>(deserializer: D) -> Result<intern_IString, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|s| intern_string_intern(&s))
    }
}

impl FromRecord for intern_IString {
    fn from_record(val: &Record) -> Result<Self, String> {
        String::from_record(val).map(|s| intern_string_intern(&s))
    }
}

impl IntoRecord for intern_IString {
    fn into_record(self) -> Record {
        intern_istring_str(&self).into_record()
    }
}

impl Mutator<intern_IString> for Record {
    fn mutate(&self, s: &mut intern_IString) -> Result<(), String> {
        let mut string = intern_istring_str(s);
        self.mutate(&mut string)?;
        *s = intern_string_intern(&string);
        Ok(())
    }
}

#[cfg(feature = "flatbuf")]
impl<'a> FromFlatBuffer<&'a str> for intern_IString {
    fn from_flatbuf(fb: &'a str) -> Response<Self> {
        Ok(intern_string_intern(&String::from_flatbuf(fb)?))
    }
}

#[cfg(feature = "flatbuf")]
impl<'b> ToFlatBuffer<'b> for intern_IString {
    type Target = fbrt::WIPOffset<&'b str>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        intern_istring_str(self).to_flatbuf(fbb)
    }
}

#[cfg(feature = "flatbuf")]
impl<'a> FromFlatBuffer<fb::__String<'a>> for intern_IString {
    fn from_flatbuf(v: fb::__String<'a>) -> Response<Self> {
        Ok(intern_string_intern(&String::from_flatbuf(v)?))
    }
}

#[cfg(feature = "flatbuf")]
impl<'b> ToFlatBufferTable<'b> for intern_IString {
    type Target = fb::__String<'b>;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        intern_istring_str(self).to_flatbuf_table(fbb)
    }
}

#[cfg(feature = "flatbuf")]
impl<'b> ToFlatBufferVectorElement<'b> for intern_IString {
    type Target = fbrt::WIPOffset<&'b str>;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}
