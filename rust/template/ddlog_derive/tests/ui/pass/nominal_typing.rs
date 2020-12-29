//! Test that records are nominally typed

use ddlog_derive::{FromRecord, IntoRecord, Mutator};
use differential_datalog::record::{IntoRecord, Mutator};
use serde::Deserialize;

fn main() {
    // The fields of struct records with the same names & types are compatible
    let mut foo = Foo {
        a: 10,
        b: "Something".to_owned(),
        c: 20,
    };
    let bar = Bar {
        a: 100,
        b: "Nothing".to_owned(),
    };

    bar.into_record().mutate(&mut foo).unwrap();
    assert_eq!(
        foo,
        Foo {
            a: 100,
            b: "Nothing".to_owned(),
            c: 20,
        },
    );

    // Tuple records with the incorrect number of fields are incompatible
    let mut baz = Baz(10, "Something".to_owned(), 20);
    let biz = Biz(100, "Nothing".to_owned());
    assert!(biz.into_record().mutate(&mut baz).is_err());

    // Tuple records with the same number of fields are compatible
    let mut baz = Baz(10, "Something".to_owned(), 20);
    let bing = Bing(100, "Nothing".to_owned(), 200);

    bing.into_record().mutate(&mut baz).unwrap();
    assert_eq!(baz, Baz(100, "Nothing".to_owned(), 200));
}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, Default, PartialEq, Deserialize)]
struct Foo {
    a: u32,
    b: String,
    c: u64,
}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, Default, PartialEq, Deserialize)]
struct Bar {
    a: u32,
    b: String,
}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, Default, PartialEq, Deserialize)]
struct Baz(u32, String, u64);

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, Default, PartialEq, Deserialize)]
struct Bing(u32, String, u64);

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, Default, PartialEq, Deserialize)]
struct Biz(u32, String);
