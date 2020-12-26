use ddlog_derive::{FromRecord, IntoRecord};
use serde::Deserialize;

fn main() {}

#[derive(FromRecord, IntoRecord, Deserialize)]
#[ddlog(rename = "Foo")]
#[ddlog(rename = "Foo")]
struct Foo {}

#[derive(IntoRecord, Deserialize)]
#[ddlog(into_record = "Bar")]
#[ddlog(rename = "Foo")]
struct Bar {}

#[derive(IntoRecord, Deserialize)]
#[ddlog(from_record = "Bar")]
#[ddlog(rename = "Foo")]
struct Baz {}

#[derive(FromRecord, Deserialize)]
#[ddlog(rename = "Foo")]
#[ddlog(into_record = "Biz")]
struct Biz {}
