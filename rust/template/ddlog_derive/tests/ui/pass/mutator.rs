use ddlog_derive::{FromRecord, IntoRecord, Mutator};
use serde::Deserialize;

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

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, PartialEq, Deserialize)]
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
