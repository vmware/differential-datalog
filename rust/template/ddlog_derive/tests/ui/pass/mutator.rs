use ddlog_derive::{FromRecord, IntoRecord, Mutator};
use differential_datalog::record::{CollectionKind, IntoRecord, Mutator, Record};
use num::BigInt;
use serde::Deserialize;
use std::{borrow::Cow, collections::BTreeMap, vec::Vec};

fn main() {
    let mutator = Record::PosStruct(
        Cow::from("ContainersEnum::Map"),
        vec![Record::Array(
            CollectionKind::Map,
            vec![Record::Tuple(vec![
                Record::PosStruct(Cow::from("Enum::Baz"), vec![]),
                Record::NamedStruct(
                    Cow::from("Struct"),
                    vec![
                        (Cow::from("foo"), Record::Int(BigInt::from(0))),
                        (Cow::from("bar"), Record::Int(BigInt::from(1))),
                        (
                            Cow::from("baz"),
                            Record::Array(CollectionKind::Vector, vec![]),
                        ),
                        (Cow::from("biz"), Record::String("foo".to_string())),
                    ],
                ),
            ])],
        )],
    );
    // Mutating an empty map inserts the new value.
    let mut map = ContainersEnum::Map(BTreeMap::default());
    mutator.mutate(&mut map).unwrap();
    assert_eq!(
        map,
        ContainersEnum::Map(
            [(
                Enum::Baz,
                Struct {
                    foo: 0,
                    bar: 1,
                    baz: vec![],
                    biz: "foo".to_string()
                }
            )]
            .iter()
            .cloned()
            .collect()
        )
    );

    // Insert to the map
    ContainersEnum::Map(
        [(Enum::Foo(0), Struct::default())]
            .iter()
            .cloned()
            .collect(),
    )
    .into_record()
    .mutate(&mut map)
    .unwrap();
    assert_eq!(
        map,
        ContainersEnum::Map(
            [
                (
                    Enum::Baz,
                    Struct {
                        foo: 0,
                        bar: 1,
                        baz: vec![],
                        biz: "foo".to_string()
                    }
                ),
                (Enum::Foo(0), Struct::default())
            ]
            .iter()
            .cloned()
            .collect()
        )
    );

    // Mutating while changing constructor overwrites the entire value.
    let mut vector = ContainersEnum::Vec(vec![]);
    mutator.mutate(&mut vector).unwrap();
    assert_eq!(
        vector,
        ContainersEnum::Map(
            [(
                Enum::Baz,
                Struct {
                    foo: 0,
                    bar: 1,
                    baz: vec![],
                    biz: "foo".to_string()
                }
            )]
            .iter()
            .cloned()
            .collect()
        )
    );

    // Mutate back to vector.
    ContainersEnum::Vec(vec![NestedMaps::default()])
        .into_record()
        .mutate(&mut vector)
        .unwrap();
    assert_eq!(vector, ContainersEnum::Vec(vec![NestedMaps::default()]));
}

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

#[derive(
    FromRecord, IntoRecord, Mutator, Debug, Clone, PartialEq, Eq, Deserialize, PartialOrd, Ord,
)]
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

#[derive(Default, FromRecord, IntoRecord, Mutator, Debug, Clone, PartialEq, Deserialize)]
struct NestedMaps {
    m: BTreeMap<BTreeMap<u64, String>, BTreeMap<String, BTreeMap<u32, u32>>>,
}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, PartialEq, Deserialize)]
enum ContainersEnum {
    Map(BTreeMap<Enum, Struct>),
    Vec(Vec<NestedMaps>),
}
