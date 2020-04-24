use arc_interner;
use differential_datalog::record;
use differential_datalog::record::*;
use serde;
use std::cmp;
use std::fmt;

#[cfg(feature = "flatbuf")]
use flatbuf::{FromFlatBuffer, ToFlatBuffer, ToFlatBufferTable, ToFlatBufferVectorElement};

/* `flatc`-generated declarations re-exported by `flatbuf.rs` */
#[cfg(feature = "flatbuf")]
use flatbuf::fb;

/* FlatBuffers runtime */
#[cfg(feature = "flatbuf")]
use flatbuffers as fbrt;

#[derive(Default, Eq, PartialOrd, PartialEq, Ord, Clone, Hash)]
pub struct internment_Intern<A>
where
    A: Eq + Send + Sync + Hash + 'static,
{
    intern: arc_interner::ArcIntern<A>,
}

impl<A: Eq + Hash + Send + Sync + 'static> internment_Intern<A> {
    pub fn new(x: A) -> internment_Intern<A> {
        internment_Intern {
            intern: arc_interner::ArcIntern::new(x),
        }
    }
    pub fn as_ref(&self) -> &A {
        self.intern.as_ref()
    }
}

pub fn internment_intern<A: Eq + Hash + Send + Sync + Clone + 'static>(
    x: &A,
) -> internment_Intern<A> {
    internment_Intern::new(x.clone())
}

pub fn internment_ival<A: Eq + Hash + Send + Sync + Clone>(x: &internment_Intern<A>) -> &A {
    x.intern.as_ref()
}

/*pub fn intern_istring_ord(s: &intern_istring) -> u32 {
    s.x
}*/

impl<A: fmt::Display + Eq + Hash + Send + Sync + Clone> fmt::Display for internment_Intern<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self.as_ref(), f)
        //record::format_ddlog_str(&intern_istring_str(self), f)
    }
}

impl<A: fmt::Debug + Eq + Hash + Send + Sync + Clone> fmt::Debug for internment_Intern<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self.as_ref(), f)
        //record::format_ddlog_str(&intern_istring_str(self), f)
    }
}

impl<A: Serialize + Eq + Hash + Send + Sync + Clone> serde::Serialize for internment_Intern<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_ref().serialize(serializer)
    }
}

impl<'de, A: Deserialize<'de> + Eq + Hash + Send + Sync + 'static> serde::Deserialize<'de>
    for internment_Intern<A>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        A::deserialize(deserializer).map(|s| internment_Intern::new(s))
    }
}

impl<A: FromRecord + Eq + Hash + Send + Sync + 'static> FromRecord for internment_Intern<A> {
    fn from_record(val: &Record) -> Result<Self, String> {
        A::from_record(val).map(|x| internment_Intern::new(x))
    }
}

impl<A: IntoRecord + Eq + Hash + Send + Sync + Clone> IntoRecord for internment_Intern<A> {
    fn into_record(self) -> Record {
        internment_ival(&self).clone().into_record()
    }
}

impl<A> Mutator<internment_Intern<A>> for Record
where
    A: Clone + Eq + Send + Sync + Hash,
    Record: Mutator<A>,
{
    fn mutate(&self, x: &mut internment_Intern<A>) -> Result<(), String> {
        let mut v = internment_ival(x).clone();
        self.mutate(&mut v)?;
        *x = internment_intern(&v);
        Ok(())
    }
}

#[cfg(feature = "flatbuf")]
impl<A, FB> FromFlatBuffer<FB> for internment_Intern<A>
where
    A: Eq + Hash + Send + Sync + 'static,
    A: FromFlatBuffer<FB>,
{
    fn from_flatbuf(fb: FB) -> Response<Self> {
        Ok(internment_Intern::new(A::from_flatbuf(fb)?))
    }
}

#[cfg(feature = "flatbuf")]
impl<'b, A, T> ToFlatBuffer<'b> for internment_Intern<A>
where
    T: 'b,
    A: Eq + Send + Sync + Hash + ToFlatBuffer<'b, Target = T>,
{
    type Target = T;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.as_ref().to_flatbuf(fbb)
    }
}

/*#[cfg(feature = "flatbuf")]
impl<'a> FromFlatBuffer<fb::__String<'a>> for intern_istring {
    fn from_flatbuf(v: fb::__String<'a>) -> Response<Self> {
        Ok(intern_string_intern(&String::from_flatbuf(v)?))
    }
}*/

#[cfg(feature = "flatbuf")]
impl<'b, A, T> ToFlatBufferTable<'b> for internment_Intern<A>
where
    T: 'b,
    A: Eq + Send + Sync + Hash + ToFlatBufferTable<'b, Target = T>,
{
    type Target = T;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        self.as_ref().to_flatbuf_table(fbb)
    }
}

#[cfg(feature = "flatbuf")]
impl<'b, A, T> ToFlatBufferVectorElement<'b> for internment_Intern<A>
where
    T: 'b + fbrt::Push + Copy,
    A: Eq + Send + Sync + Hash + ToFlatBufferVectorElement<'b, Target = T>,
{
    type Target = T;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.as_ref().to_flatbuf_vector_element(fbb)
    }
}

pub fn internment_istring_join(strings: &std_Vec<internment_istring>, sep: &String) -> String {
    strings
        .x
        .iter()
        .map(|s| s.as_ref())
        .cloned()
        .collect::<Vec<String>>()
        .join(sep.as_str())
}

pub fn internment_istring_split(s: &internment_istring, sep: &String) -> std_Vec<String> {
    std_Vec {
        x: s.as_ref().split(sep).map(|x| x.to_owned()).collect(),
    }
}

pub fn internment_istring_contains(s1: &internment_istring, s2: &String) -> bool {
    s1.as_ref().contains(s2.as_str())
}

pub fn internment_istring_substr(
    s: &internment_istring,
    start: &std_usize,
    end: &std_usize,
) -> String {
    let len = s.as_ref().len();
    let from = cmp::min(*start as usize, len);
    let to = cmp::max(from, cmp::min(*end as usize, len));
    s.as_ref()[from..to].to_string()
}

pub fn internment_istring_replace(s: &internment_istring, from: &String, to: &String) -> String {
    s.as_ref().replace(from, to)
}

pub fn internment_istring_starts_with(s: &internment_istring, prefix: &String) -> bool {
    s.as_ref().starts_with(prefix)
}

pub fn internment_istring_ends_with(s: &internment_istring, suffix: &String) -> bool {
    s.as_ref().ends_with(suffix)
}

pub fn internment_istring_trim(s: &internment_istring) -> String {
    s.as_ref().trim().to_string()
}

pub fn internment_istring_len(s: &internment_istring) -> std_usize {
    s.as_ref().len() as std_usize
}

pub fn internment_istring_to_bytes(s: &internment_istring) -> std_Vec<u8> {
    std_Vec::from(s.as_ref().as_bytes())
}

pub fn internment_istring_to_lowercase(s: &internment_istring) -> String {
    s.as_ref().to_lowercase()
}

pub fn internment_istring_to_uppercase(s: &internment_istring) -> String {
    s.as_ref().to_uppercase()
}

pub fn internment_istring_reverse(s: &internment_istring) -> String {
    s.as_ref().chars().rev().collect()
}
