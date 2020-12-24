use ddlog_derive::{FromRecord, IntoRecord};
use differential_datalog::record::{FromRecord, IntoRecord};
use std::borrow::Cow;

fn main() {
    let default = DefaultRecordNames {
        foo: 10,
        bar: 20,
        baz: vec![10],
        biz: "this is a random string".to_owned(),
    };
    let default_record = default.clone().into_record();
    assert_eq!(
        DefaultRecordNames::from_record(&default_record),
        Ok(default),
    );
    assert!(default_record.is_struct());
    assert_eq!(
        default_record.struct_constructor(),
        Some(&Cow::Borrowed("DefaultRecordNames")),
    );
    assert_eq!(
        default_record.named_struct_fields(),
        Some(
            &[
                (Cow::Borrowed("foo"), 10u32.into_record()),
                (Cow::Borrowed("bar"), 20u64.into_record()),
                (Cow::Borrowed("baz"), vec![10u8].into_record()),
                (
                    Cow::Borrowed("biz"),
                    "this is a random string".into_record(),
                ),
            ][..]
        ),
    );

    let renamed = RenamedStruct {
        foo: 10,
        bar: 20,
        baz: vec![10],
        biz: "this is a random string".to_owned(),
    };
    let renamed_record = renamed.clone().into_record();
    assert_eq!(RenamedStruct::from_record(&renamed_record), Ok(renamed),);
    assert!(renamed_record.is_struct());
    assert_eq!(
        renamed_record.struct_constructor(),
        Some(&Cow::Borrowed("some::random::path::RenamedStruct")),
    );
    assert_eq!(
        renamed_record.named_struct_fields(),
        Some(
            &[
                (Cow::Borrowed("i am a field"), 10u32.into_record()),
                (Cow::Borrowed("so am i"), 20u64.into_record()),
                (Cow::Borrowed("this works"), vec![10u8].into_record()),
                (
                    Cow::Borrowed("biz"),
                    "this is a random string".into_record(),
                ),
            ][..]
        ),
    );

    let empty = EmptyStruct {};
    let empty_record = empty.clone().into_record();
    assert_eq!(EmptyStruct::from_record(&empty_record), Ok(empty));
    assert!(empty_record.is_struct());
    assert_eq!(
        empty_record.struct_constructor(),
        Some(&Cow::Borrowed("EmptyStruct")),
    );
    assert_eq!(empty_record.named_struct_fields(), Some(&[][..]));

    let renamed_empty = RenamedEmptyStruct {};
    let renamed_empty_record = renamed_empty.clone().into_record();
    assert_eq!(
        RenamedEmptyStruct::from_record(&renamed_empty_record),
        Ok(renamed_empty),
    );
    assert!(renamed_empty_record.is_struct());
    assert_eq!(
        renamed_empty_record.struct_constructor(),
        Some(&Cow::Borrowed("so::alone::and::Empty")),
    );
    assert_eq!(renamed_empty_record.named_struct_fields(), Some(&[][..]));

    let tuple = TupleStruct(100, "foobar".to_owned());
    let tuple_record = tuple.clone().into_record();
    assert_eq!(TupleStruct::from_record(&tuple_record), Ok(tuple));
    assert!(tuple_record.is_struct());
    assert_eq!(
        tuple_record.struct_constructor(),
        Some(&Cow::Borrowed("TupleStruct")),
    );
    assert_eq!(
        tuple_record.named_struct_fields(),
        Some(
            &[
                (Cow::Borrowed("0"), 100u32.into_record()),
                (Cow::Borrowed("1"), "foobar".into_record()),
            ][..]
        )
    );
}

#[derive(FromRecord, IntoRecord, Debug, Clone, Default, PartialEq)]
struct DefaultRecordNames {
    foo: u32,
    bar: u64,
    baz: Vec<u8>,
    biz: String,
}

#[derive(FromRecord, IntoRecord, Debug, Clone, Default, PartialEq)]
#[ddlog(rename = "some::random::path::RenamedStruct")]
struct RenamedStruct {
    #[ddlog(rename = "i am a field")]
    foo: u32,
    #[ddlog(rename = "so am i")]
    bar: u64,
    #[ddlog(from_record = "this works")]
    #[ddlog(into_record = "this works")]
    baz: Vec<u8>,
    biz: String,
}

#[derive(FromRecord, IntoRecord, Debug, Clone, Default, PartialEq)]
struct EmptyStruct {}

#[derive(FromRecord, IntoRecord, Debug, Clone, Default, PartialEq)]
#[ddlog(rename = "so::alone::and::Empty")]
struct RenamedEmptyStruct {}

#[derive(FromRecord, IntoRecord, Debug, Clone, Default, PartialEq)]
struct TupleStruct(u32, String);
