use ddlog_derive::{FromRecord, IntoRecord, Mutator};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    vec::Vec,
};

fn main() {}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, Default, PartialEq, Deserialize)]
struct Struct {
    foo: u32,
    bar: u64,
    baz: Vec<u8>,
    biz: String,
}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, Default, PartialEq, Deserialize)]
struct EmptyStruct {}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, Default, PartialEq, Deserialize)]
struct UnitStruct;

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, Default, PartialEq, Deserialize)]
struct TupleStruct(u32, u64, Vec<u8>, String);

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, PartialEq, Eq, Deserialize, PartialOrd, Ord)]
enum Enum {
    Foo(u32),
    Bar { foo: u64 },
    Baz,
}

impl Default for Enum {
    fn default() -> Self {
        Self::Baz
    }
}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, PartialEq, Deserialize)]
struct NestedMaps {
    m: BTreeMap<BTreeMap<u64, String>, BTreeMap<String, BTreeMap<u32, u32>>>
}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, PartialEq, Deserialize)]
enum ContainersEnum {
    Map(BTreeMap<Enum, Struct>),
    Vec(Vec<NestedMaps>)
}
