use ddlog_derive::{FromRecord, IntoRecord};
use differential_datalog::record::{FromRecord, IntoRecord};
use std::borrow::Cow;

fn main() {
    default_record_names();
    renamed_struct();
}

#[derive(FromRecord, IntoRecord, Debug, Clone, PartialEq)]
enum DefaultRecordNames {
    Foo,
    Bar(u32, u64),
    Baz { bing: u32, bong: String },
}

fn default_record_names() {
    let default = DefaultRecordNames::Foo;
    let default_record = default.clone().into_record();
    assert_eq!(
        DefaultRecordNames::from_record(&default_record),
        Ok(default),
    );
    assert!(default_record.is_struct());
    assert_eq!(
        default_record.struct_constructor(),
        Some(&Cow::Borrowed("DefaultRecordNames::Foo")),
    );
    assert_eq!(default_record.named_struct_fields(), Some(&[][..]));

    let default = DefaultRecordNames::Bar(10, 20);
    let default_record = default.clone().into_record();
    assert_eq!(
        DefaultRecordNames::from_record(&default_record),
        Ok(default),
    );
    assert!(default_record.is_struct());
    assert_eq!(
        default_record.struct_constructor(),
        Some(&Cow::Borrowed("DefaultRecordNames::Bar")),
    );
    assert_eq!(
        default_record.named_struct_fields(),
        Some(
            &[
                (Cow::Borrowed("0"), 10u32.into_record()),
                (Cow::Borrowed("1"), 20u64.into_record()),
            ][..]
        ),
    );

    let default = DefaultRecordNames::Baz {
        bing: 30,
        bong: "this is a random string".to_owned(),
    };
    let default_record = default.clone().into_record();
    assert_eq!(
        DefaultRecordNames::from_record(&default_record),
        Ok(default),
    );
    assert!(default_record.is_struct());
    assert_eq!(
        default_record.struct_constructor(),
        Some(&Cow::Borrowed("DefaultRecordNames::Baz")),
    );
    assert_eq!(
        default_record.named_struct_fields(),
        Some(
            &[
                (Cow::Borrowed("bing"), 30u32.into_record()),
                (
                    Cow::Borrowed("bong"),
                    "this is a random string".into_record(),
                ),
            ][..]
        ),
    );
}

#[derive(FromRecord, IntoRecord, Debug, Clone, PartialEq)]
#[ddlog(rename = "some::random::path::RenamedEnum")]
enum RenamedEnum {
    #[ddlog(rename = "i am a field")]
    Foo,
    #[ddlog(rename = "so am i")]
    Bar(u32, u64),
    #[ddlog(from_record = "this works")]
    #[ddlog(into_record = "this works")]
    Baz { bing: u32, bong: String },
}

fn renamed_struct() {
    let renamed = RenamedEnum::Foo;
    let renamed_record = renamed.clone().into_record();
    assert_eq!(RenamedEnum::from_record(&renamed_record), Ok(renamed));
    assert!(renamed_record.is_struct());
    assert_eq!(
        renamed_record.struct_constructor(),
        Some(&Cow::Borrowed(
            "some::random::path::RenamedEnum::i am a field",
        )),
    );
    assert_eq!(renamed_record.named_struct_fields(), Some(&[][..]));

    let renamed = RenamedEnum::Bar(10, 20);
    let renamed_record = renamed.clone().into_record();
    assert_eq!(RenamedEnum::from_record(&renamed_record), Ok(renamed));
    assert!(renamed_record.is_struct());
    assert_eq!(
        renamed_record.struct_constructor(),
        Some(&Cow::Borrowed("some::random::path::RenamedEnum::so am i")),
    );
    assert_eq!(
        renamed_record.named_struct_fields(),
        Some(
            &[
                (Cow::Borrowed("0"), 10u32.into_record()),
                (Cow::Borrowed("1"), 20u64.into_record()),
            ][..]
        ),
    );

    let renamed = RenamedEnum::Baz {
        bing: 30,
        bong: "this is a random string".to_owned(),
    };
    let renamed_record = renamed.clone().into_record();
    assert_eq!(RenamedEnum::from_record(&renamed_record), Ok(renamed));
    assert!(renamed_record.is_struct());
    assert_eq!(
        renamed_record.struct_constructor(),
        Some(&Cow::Borrowed(
            "some::random::path::RenamedEnum::this works"
        )),
    );
    assert_eq!(
        renamed_record.named_struct_fields(),
        Some(
            &[
                (Cow::Borrowed("bing"), 30u32.into_record()),
                (
                    Cow::Borrowed("bong"),
                    "this is a random string".into_record(),
                ),
            ][..]
        ),
    );
}
