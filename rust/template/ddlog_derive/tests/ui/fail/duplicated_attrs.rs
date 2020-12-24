use ddlog_derive::{FromRecord, IntoRecord};

fn main() {}

#[derive(FromRecord, IntoRecord)]
#[ddlog(rename = "Foo")]
#[ddlog(rename = "Foo")]
struct Foo {}

#[derive(IntoRecord)]
#[ddlog(into_record = "Bar")]
#[ddlog(rename = "Foo")]
struct Bar {}

#[derive(IntoRecord)]
#[ddlog(from_record = "Bar")]
#[ddlog(rename = "Foo")]
struct Baz {}

#[derive(FromRecord)]
#[ddlog(rename = "Foo")]
#[ddlog(into_record = "Biz")]
struct Biz {}
