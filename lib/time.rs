use differential_datalog::record;
use std::fmt::Display;

//////////////////////////// Time //////////////////////////////////
#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct TimeWrapper {
    val: ::time::Time,
}
pub type Time = TimeWrapper;

impl Default for TimeWrapper {
    fn default() -> Self {
        let mid = ::time::Time::midnight();
        TimeWrapper { val: mid }
    }
}

pub fn res2std_time_wrap<E: Display>(
    r: ::std::result::Result<::time::Time, E>,
) -> crate::ddlog_std::Result<Time, String> {
    match (r) {
        Ok(res) => {
            let t = TimeWrapper { val: res };
            crate::ddlog_std::Result::Ok { res: t }
        }
        Err(e) => crate::ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn try_from_hms(h: &u8, m: &u8, s: &u8) -> crate::ddlog_std::Result<Time, String> {
    res2std_time_wrap(::time::Time::try_from_hms(*h, *m, *s))
}

pub fn try_from_hms_milli(
    h: &u8,
    m: &u8,
    s: &u8,
    ms: &u16,
) -> crate::ddlog_std::Result<Time, String> {
    res2std_time_wrap(::time::Time::try_from_hms_milli(*h, *m, *s, *ms))
}

pub fn try_from_hms_micro(
    h: &u8,
    m: &u8,
    s: &u8,
    mc: &u32,
) -> crate::ddlog_std::Result<Time, String> {
    res2std_time_wrap(::time::Time::try_from_hms_micro(*h, *m, *s, *mc))
}

pub fn try_from_hms_nano(
    h: &u8,
    m: &u8,
    s: &u8,
    mc: &u32,
) -> crate::ddlog_std::Result<Time, String> {
    res2std_time_wrap(::time::Time::try_from_hms_nano(*h, *m, *s, *mc))
}

pub fn hour(t: &Time) -> u8 {
    ::time::Time::hour(t.val)
}

pub fn minute(t: &Time) -> u8 {
    ::time::Time::minute(t.val)
}

pub fn second(t: &Time) -> u8 {
    ::time::Time::second(t.val)
}

pub fn millisecond(t: &Time) -> u16 {
    ::time::Time::millisecond(t.val)
}

pub fn microsecond(t: &Time) -> u32 {
    ::time::Time::microsecond(t.val)
}

pub fn nanosecond(t: &Time) -> u32 {
    ::time::Time::nanosecond(t.val)
}

const default_time_format: &str = "%T.%N";

pub fn time2string(t: &Time) -> String {
    t.val.format(default_time_format)
}

pub fn string2time(s: &String) -> crate::ddlog_std::Result<Time, String> {
    res2std_time_wrap(::time::Time::parse(s, default_time_format))
}

pub fn midnight() -> Time {
    TimeWrapper {
        val: ::time::Time::midnight(),
    }
}

pub fn time_parse(s: &String, format: &String) -> crate::ddlog_std::Result<Time, String> {
    res2std_time_wrap(::time::Time::parse(s, format))
}

pub fn time_format(t: &Time, format: &String) -> String {
    t.val.format(format)
}

impl FromRecord for Time {
    fn from_record(val: &record::Record) -> ::std::result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => match (::time::Time::parse(s, default_time_format)) {
                Ok(s) => Ok(TimeWrapper { val: s }),
                Err(e) => Err(format!("{}", e)),
            },
            _ => Err(String::from("Unexpected type")),
        }
    }
}

impl IntoRecord for Time {
    fn into_record(self) -> record::Record {
        record::Record::String(time2string(&self))
    }
}

impl record::Mutator<Time> for record::Record {
    fn mutate(&self, t: &mut Time) -> ::std::result::Result<(), String> {
        *t = Time::from_record(self)?;
        Ok(())
    }
}

//////////////////////////// Date //////////////////////////////////

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct DateWrapper {
    val: ::time::Date,
}
pub type Date = DateWrapper;

impl Default for DateWrapper {
    fn default() -> Self {
        let mid = ::time::Date::try_from_ymd(0, 0, 0).unwrap();
        DateWrapper { val: mid }
    }
}

pub fn res2std_date_wrap<E: Display>(
    r: ::std::result::Result<::time::Date, E>,
) -> crate::ddlog_std::Result<Date, String> {
    match (r) {
        Ok(res) => {
            let t = DateWrapper { val: res };
            crate::ddlog_std::Result::Ok { res: t }
        }
        Err(e) => crate::ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn try_from_ymd(year: &i32, month: &u8, day: &u8) -> crate::ddlog_std::Result<Date, String> {
    res2std_date_wrap(::time::Date::try_from_ymd(*year, *month, *day))
}

pub fn try_from_yo(year: &i32, ordinal: &u16) -> crate::ddlog_std::Result<Date, String> {
    res2std_date_wrap(::time::Date::try_from_yo(*year, *ordinal))
}

fn convert_weekday_from_ddlog(weekday: &Weekday) -> ::time::Weekday {
    match (*weekday) {
        Weekday::Monday => ::time::Weekday::Monday,
        Weekday::Tuesday => ::time::Weekday::Tuesday,
        Weekday::Wednesday => ::time::Weekday::Wednesday,
        Weekday::Thursday => ::time::Weekday::Thursday,
        Weekday::Friday => ::time::Weekday::Friday,
        Weekday::Saturday => ::time::Weekday::Saturday,
        Weekday::Sunday => ::time::Weekday::Sunday,
    }
}

fn convert_weekday_to_ddlog(weekday: ::time::Weekday) -> Weekday {
    match (weekday) {
        ::time::Weekday::Monday => Weekday::Monday,
        ::time::Weekday::Tuesday => Weekday::Tuesday,
        ::time::Weekday::Wednesday => Weekday::Wednesday,
        ::time::Weekday::Thursday => Weekday::Thursday,
        ::time::Weekday::Friday => Weekday::Friday,
        ::time::Weekday::Saturday => Weekday::Saturday,
        ::time::Weekday::Sunday => Weekday::Sunday,
    }
}

pub fn try_from_iso_ywd(
    year: &i32,
    week: &u8,
    weekday: &Weekday,
) -> crate::ddlog_std::Result<Date, String> {
    res2std_date_wrap(::time::Date::try_from_iso_ywd(
        *year,
        *week,
        convert_weekday_from_ddlog(weekday),
    ))
}

pub fn year(date: &Date) -> i32 {
    ::time::Date::year((*date).val)
}

pub fn month(date: &Date) -> u8 {
    ::time::Date::month((*date).val)
}

pub fn day(date: &Date) -> u8 {
    ::time::Date::day((*date).val)
}

pub fn ordinal(date: &Date) -> u16 {
    ::time::Date::ordinal((*date).val)
}

pub fn week(date: &Date) -> u8 {
    ::time::Date::week((*date).val)
}

pub fn sunday_based_week(date: &Date) -> u8 {
    ::time::Date::sunday_based_week((*date).val)
}

pub fn monday_based_week(date: &Date) -> u8 {
    ::time::Date::monday_based_week((*date).val)
}

pub fn weekday(date: &Date) -> Weekday {
    convert_weekday_to_ddlog(::time::Date::weekday((*date).val))
}

pub fn next_day(date: &Date) -> Date {
    DateWrapper {
        val: ::time::Date::next_day((*date).val),
    }
}

pub fn previous_day(date: &Date) -> Date {
    DateWrapper {
        val: ::time::Date::previous_day((*date).val),
    }
}

pub fn julian_day(date: &Date) -> i64 {
    ::time::Date::julian_day((*date).val)
}

pub fn from_julian_day(julian_day: &i64) -> Date {
    DateWrapper {
        val: ::time::Date::from_julian_day(*julian_day),
    }
}

const default_date_format: &str = "%Y-%m-%d";

impl FromRecord for Date {
    fn from_record(val: &record::Record) -> ::std::result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => match (::time::Date::parse(s, default_date_format)) {
                Ok(s) => Ok(DateWrapper { val: s }),
                Err(e) => Err(format!("{}", e)),
            },
            _ => Err(String::from("Unexpected type")),
        }
    }
}

pub fn date2string(t: &Date) -> String {
    t.val.format(default_date_format)
}

pub fn date_format(d: &Date, format: &String) -> String {
    d.val.format(format)
}

pub fn string2date(s: &String) -> crate::ddlog_std::Result<Date, String> {
    res2std_date_wrap(::time::Date::parse(s, default_date_format))
}

pub fn date_parse(s: &String, format: &String) -> crate::ddlog_std::Result<Date, String> {
    res2std_date_wrap(::time::Date::parse(s, format))
}

impl IntoRecord for Date {
    fn into_record(self) -> record::Record {
        record::Record::String(date2string(&self))
    }
}

impl record::Mutator<Date> for record::Record {
    fn mutate(&self, t: &mut Date) -> ::std::result::Result<(), String> {
        *t = Date::from_record(self)?;
        Ok(())
    }
}

//////////////////////////////////////// DateTime //////////////////////////////////////

const defaultDateTimeFormat: &str = "%Y-%m-%dT%T";

pub fn datetime_parse(s: &String, format: &String) -> crate::ddlog_std::Result<DateTime, String> {
    let prim = ::time::PrimitiveDateTime::parse(s, format);
    match (prim) {
        Ok(res) => {
            let dt = DateTime {
                date: DateWrapper { val: res.date() },
                time: TimeWrapper { val: res.time() },
            };
            crate::ddlog_std::Result::Ok { res: dt }
        }
        Err(m) => crate::ddlog_std::Result::Err {
            err: format!("{}", m),
        },
    }
}

pub fn dateTime2string(d: &DateTime) -> String {
    let prim = ::time::PrimitiveDateTime::new((*d).date.val, (*d).time.val);
    prim.format(defaultDateTimeFormat)
}

pub fn string2datetime(s: &String) -> crate::ddlog_std::Result<DateTime, String> {
    datetime_parse(s, &String::from(defaultDateTimeFormat))
}

pub fn datetime_format(d: &DateTime, format: &String) -> String {
    let dt = ::time::PrimitiveDateTime::new((*d).date.val, (*d).time.val);
    dt.format(format)
}

pub fn datetime_from_unix_timestamp(timestamp: &i64) -> DateTime {
    let odt = ::time::OffsetDateTime::from_unix_timestamp(*timestamp);
    DateTime {
        date: DateWrapper { val: odt.date() },
        time: TimeWrapper { val: odt.time() },
    }
}
