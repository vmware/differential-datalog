use ddlog_derive::{FromRecord, IntoRecord, Mutator};

fn main() {}

#[derive(FromRecord, IntoRecord, Mutator)]
union Foo {
    bar: u32,
    baz: u64,
}
