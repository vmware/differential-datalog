use ddlog_derive::{FromRecord, IntoRecord};

fn main() {}

#[derive(FromRecord)]
#[ddlog(flom_record = "Foo")]
struct Foo {}

#[derive(IntoRecord)]
#[ddlog(into_recrd = "Bar")]
struct Bar {}
