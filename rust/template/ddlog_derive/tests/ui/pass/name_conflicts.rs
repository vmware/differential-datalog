use ddlog_derive::{FromRecord, IntoRecord, Mutator};
use serde::Deserialize;

fn main() {}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, PartialEq, Deserialize)]
struct Struct {
    args: u64,
    constructor: u64,
}

#[derive(FromRecord, IntoRecord, Mutator, Debug, Clone, PartialEq, Deserialize)]
enum Enum {
    Variant { args: u64, constructor: u64 },
}
