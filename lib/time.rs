use std::fmt::Display;
use time::*;

//////////////////////////// Time //////////////////////////////////
#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct TimeWrapper {
    val: time::Time,
}
pub type time_Time = TimeWrapper;

impl Default for TimeWrapper {
    fn default() -> Self {
        let mid = Time::midnight();
        TimeWrapper { val: mid }
    }
}

pub fn res2std_time_wrap<E: Display>(
    r: result::Result<time::Time, E>,
) -> std_Result<time_Time, String> {
    match (r) {
        Ok(res) => {
            let t = TimeWrapper { val: res };
            std_Result::std_Ok { res: t }
        }
        Err(e) => std_Result::std_Err {
            err: format!("{}", e),
        },
    }
}

pub fn time_try_from_hms(h: &u8, m: &u8, s: &u8) -> std_Result<time_Time, String> {
    res2std_time_wrap(Time::try_from_hms(*h, *m, *s))
}

pub fn time_try_from_hms_milli(h: &u8, m: &u8, s: &u8, ms: &u16) -> std_Result<time_Time, String> {
    res2std_time_wrap(Time::try_from_hms_milli(*h, *m, *s, *ms))
}

pub fn time_try_from_hms_micro(h: &u8, m: &u8, s: &u8, mc: &u32) -> std_Result<time_Time, String> {
    res2std_time_wrap(Time::try_from_hms_micro(*h, *m, *s, *mc))
}

pub fn time_try_from_hms_nano(h: &u8, m: &u8, s: &u8, mc: &u32) -> std_Result<time_Time, String> {
    res2std_time_wrap(Time::try_from_hms_nano(*h, *m, *s, *mc))
}

pub fn time_hour(t: &time_Time) -> u8 {
    Time::hour(t.val)
}

pub fn time_minute(t: &time_Time) -> u8 {
    Time::minute(t.val)
}

pub fn time_second(t: &time_Time) -> u8 {
    Time::second(t.val)
}

pub fn time_millisecond(t: &time_Time) -> u16 {
    Time::millisecond(t.val)
}

pub fn time_microsecond(t: &time_Time) -> u32 {
    Time::microsecond(t.val)
}

pub fn time_nanosecond(t: &time_Time) -> u32 {
    Time::nanosecond(t.val)
}

const default_time_format: &str = "%T.%N";

pub fn time_time2string(t: &time_Time) -> String {
    t.val.format(default_time_format)
}

pub fn time_string2time(s: &String) -> std_Result<time_Time, String> {
    res2std_time_wrap(Time::parse(s, default_time_format))
}

pub fn time_midnight() -> time_Time {
    TimeWrapper {
        val: Time::midnight(),
    }
}

pub fn time_time_parse(s: &String, format: &String) -> std_Result<time_Time, String> {
    res2std_time_wrap(Time::parse(s, format))
}

impl FromRecord for time_Time {
    fn from_record(val: &record::Record) -> result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => match (Time::parse(s, default_time_format)) {
                Ok(s) => Ok(TimeWrapper { val: s }),
                Err(e) => Err(format!("{}", e)),
            },
            _ => Err(String::from("Unexpected type")),
        }
    }
}

impl IntoRecord for time_Time {
    fn into_record(self) -> record::Record {
        record::Record::String(time_time2string(&self))
    }
}

impl record::Mutator<time_Time> for record::Record {
    fn mutate(&self, t: &mut time_Time) -> result::Result<(), String> {
        unimplemented!("time_Time::mutator")
    }
}

//////////////////////////// Date //////////////////////////////////

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct DateWrapper {
    val: time::Date,
}
pub type time_Date = DateWrapper;

impl Default for DateWrapper {
    fn default() -> Self {
        let mid = Date::try_from_ymd(0, 0, 0).unwrap();
        DateWrapper { val: mid }
    }
}

pub fn res2std_date_wrap<E: Display>(
    r: result::Result<time::Date, E>,
) -> std_Result<time_Date, String> {
    match (r) {
        Ok(res) => {
            let t = DateWrapper { val: res };
            std_Result::std_Ok { res: t }
        }
        Err(e) => std_Result::std_Err {
            err: format!("{}", e),
        },
    }
}

pub fn time_try_from_ymd(year: &i32, month: &u8, day: &u8) -> std_Result<time_Date, String> {
    res2std_date_wrap(Date::try_from_ymd(*year, *month, *day))
}

pub fn time_try_from_yo(year: &i32, ordinal: &u16) -> std_Result<time_Date, String> {
    res2std_date_wrap(Date::try_from_yo(*year, *ordinal))
}

fn convert_weekday_from_ddlog(weekday: &time_Weekday) -> time::Weekday {
    match (*weekday) {
        time_Weekday::time_Monday => Weekday::Monday,
        time_Weekday::time_Tuesday => Weekday::Tuesday,
        time_Weekday::time_Wednesday => Weekday::Wednesday,
        time_Weekday::time_Thursday => Weekday::Thursday,
        time_Weekday::time_Friday => Weekday::Friday,
        time_Weekday::time_Saturday => Weekday::Saturday,
        time_Weekday::time_Sunday => Weekday::Sunday,
    }
}

fn convert_weekday_to_ddlog(weekday: time::Weekday) -> time_Weekday {
    match (weekday) {
        Weekday::Monday => time_Weekday::time_Monday,
        Weekday::Tuesday => time_Weekday::time_Tuesday,
        Weekday::Wednesday => time_Weekday::time_Wednesday,
        Weekday::Thursday => time_Weekday::time_Thursday,
        Weekday::Friday => time_Weekday::time_Friday,
        Weekday::Saturday => time_Weekday::time_Saturday,
        Weekday::Sunday => time_Weekday::time_Sunday,
    }
}

pub fn time_try_from_iso_ywd(
    year: &i32,
    week: &u8,
    weekday: &time_Weekday,
) -> std_Result<time_Date, String> {
    res2std_date_wrap(Date::try_from_iso_ywd(
        *year,
        *week,
        convert_weekday_from_ddlog(weekday),
    ))
}

pub fn time_year(date: &time_Date) -> i32 {
    Date::year((*date).val)
}

pub fn time_month(date: &time_Date) -> u8 {
    Date::month((*date).val)
}

pub fn time_day(date: &time_Date) -> u8 {
    Date::day((*date).val)
}

pub fn time_ordinal(date: &time_Date) -> u16 {
    Date::ordinal((*date).val)
}

pub fn time_week(date: &time_Date) -> u8 {
    Date::week((*date).val)
}

pub fn time_sunday_based_week(date: &time_Date) -> u8 {
    Date::sunday_based_week((*date).val)
}

pub fn time_monday_based_week(date: &time_Date) -> u8 {
    Date::monday_based_week((*date).val)
}

pub fn time_weekday(date: &time_Date) -> time_Weekday {
    convert_weekday_to_ddlog(Date::weekday((*date).val))
}

pub fn time_next_day(date: &time_Date) -> time_Date {
    DateWrapper {
        val: Date::next_day((*date).val),
    }
}

pub fn time_previous_day(date: &time_Date) -> time_Date {
    DateWrapper {
        val: Date::previous_day((*date).val),
    }
}

pub fn time_julian_day(date: &time_Date) -> i64 {
    Date::julian_day((*date).val),
}

pub fn time_from_julian_day(julian_day: &i64) -> time_Date {
    DateWrapper {
        val: Date::from_julian_day(*julian_day),
    }
}

const default_date_format: &str = "%Y/%m/%d";

impl FromRecord for time_Date {
    fn from_record(val: &record::Record) -> result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => match (Date::parse(s, default_date_format)) {
                Ok(s) => Ok(DateWrapper { val: s }),
                Err(e) => Err(format!("{}", e)),
            },
            _ => Err(String::from("Unexpected type")),
        }
    }
}

pub fn time_date2string(t: &time_Date) -> String {
    t.val.format(default_date_format)
}

pub fn time_string2date(s: &String) -> std_Result<time_Date, String> {
    res2std_date_wrap(Date::parse(s, default_date_format))
}

pub fn time_date_parse(s: &String, format: &String) -> std_Result<time_Date, String> {
    res2std_date_wrap(Date::parse(s, format))
}

impl IntoRecord for time_Date {
    fn into_record(self) -> record::Record {
        record::Record::String(time_date2string(&self))
    }
}

impl record::Mutator<time_Date> for record::Record {
    fn mutate(&self, t: &mut time_Date) -> result::Result<(), String> {
        unimplemented!("time_Date::mutator")
    }
}

//////////////////////////////////////// DateTime //////////////////////////////////////

const defaultDateTimeFormat: &str = "%Y-%m-%dT%T";

pub fn time_datetime_parse(s: &String, format: &String) -> std_Result<time_DateTime, String> {
    let prim = PrimitiveDateTime::parse(s, format);
    match (prim) {
        Ok(res) => {
            let dt = time_DateTime {
                date: DateWrapper { val: res.date() },
                time: TimeWrapper { val: res.time() },
            };
            std_Result::std_Ok { res: dt }
        }
        Err(m) => std_Result::std_Err {
            err: format!("{}", m),
        },
    }
}

pub fn time_dateTime2string(d: &time_DateTime) -> String {
    let prim = PrimitiveDateTime::new((*d).date.val, (*d).time.val);
    prim.format(defaultDateTimeFormat)
}

pub fn time_string2datetime(s: &String) -> std_Result<time_DateTime, String> {
    time_datetime_parse(s, &String::from(defaultDateTimeFormat))
}

pub fn time_datetime_format(d: &time_DateTime, format: &String) -> String {
    let dt = PrimitiveDateTime::new((*d).date.val, (*d).time.val);
    dt.format(format)
}
