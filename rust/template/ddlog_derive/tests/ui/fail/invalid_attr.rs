use ddlog_derive::{FromRecord, IntoRecord};
use serde::Deserialize;

fn main() {}

#[derive(FromRecord, Deserialize)]
#[ddlog(flom_record = "Foo")]
struct Foo {}

#[derive(IntoRecord, Deserialize)]
#[ddlog(into_recrd = "Bar")]
struct Bar {}
