/*
Copyright (c) 2021 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use std::panic;
use chrono::{Datelike, FixedOffset, NaiveDateTime, TimeZone, Timelike};
use differential_datalog::record;
use std::fmt::Write;

//////////////////////////// Time //////////////////////////////////
#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct TimeWrapper {
    val: ::chrono::NaiveTime,
}
pub type Time = TimeWrapper;

impl Default for TimeWrapper {
    fn default() -> Self {
        let mid = ::chrono::NaiveTime::from_hms(0, 0, 0);
        TimeWrapper { val: mid }
    }
}

pub fn time_option_to_result(r: Option<::chrono::NaiveTime>) -> ddlog_std::Result<Time, String> {
    match (r) {
        Some(res) => ddlog_std::Result::Ok {
            res: TimeWrapper { val: res },
        },
        None => ddlog_std::Result::Err {
            err: "illegal time value".to_string(),
        },
    }
}

pub fn try_from_hms(h: &u8, m: &u8, s: &u8) -> ddlog_std::Result<Time, String> {
    time_option_to_result(::chrono::NaiveTime::from_hms_opt(
        *h as u32, *m as u32, *s as u32,
    ))
}

pub fn try_from_hms_milli(h: &u8, m: &u8, s: &u8, ms: &u16) -> ddlog_std::Result<Time, String> {
    time_option_to_result(::chrono::NaiveTime::from_hms_milli_opt(
        *h as u32, *m as u32, *s as u32, *ms as u32,
    ))
}

pub fn try_from_hms_micro(h: &u8, m: &u8, s: &u8, mc: &u32) -> ddlog_std::Result<Time, String> {
    time_option_to_result(::chrono::NaiveTime::from_hms_micro_opt(
        *h as u32, *m as u32, *s as u32, *mc as u32,
    ))
}

pub fn try_from_hms_nano(h: &u8, m: &u8, s: &u8, mc: &u32) -> ddlog_std::Result<Time, String> {
    time_option_to_result(::chrono::NaiveTime::from_hms_nano_opt(
        *h as u32, *m as u32, *s as u32, *mc as u32,
    ))
}

pub fn hour(t: &Time) -> u8 {
    t.val.hour() as u8
}

pub fn minute(t: &Time) -> u8 {
    t.val.minute() as u8
}

pub fn second(t: &Time) -> u8 {
    t.val.second() as u8
}

pub fn millisecond(t: &Time) -> u16 {
    (t.val.nanosecond() / 1_000_000) as u16
}

pub fn microsecond(t: &Time) -> u32 {
    t.val.nanosecond() / 1_000
}

pub fn nanosecond(t: &Time) -> u32 {
    t.val.nanosecond()
}

const default_time_format: &str = "%T.%f";

pub fn midnight() -> Time {
    TimeWrapper {
        val: ::chrono::NaiveTime::from_hms(0, 0, 0),
    }
}

pub fn time_parse(s: &String, format: &String) -> ddlog_std::Result<Time, String> {
    match (::chrono::NaiveTime::parse_from_str(s, format)) {
        Ok(t) => ddlog_std::Result::Ok {
            res: TimeWrapper { val: t },
        },
        Err(e) => ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn result_from_delayed_format<'a>(d: chrono::format::DelayedFormat<chrono::format::strftime::StrftimeItems<'a>>, format: &String) -> ddlog_std::Result<String, String> {
    let mut buffer = String::new();
    match write!(&mut buffer, "{}", d) {
        Ok(t) => ddlog_std::Result::Ok {
            res: buffer,
        },
        Err(_) => ddlog_std::Result::Err {
            err: format!("Error in format string '{}'", format),
        },
    }
}

pub fn time_format(t: &Time, format: &String) -> ddlog_std::Result<String, String> {
    result_from_delayed_format(t.val.format(format), format)
}

pub fn time2string(t: &Time) -> String {
    // This should not panic since we know the format string is correct
    t.val.format(&default_time_format.to_string()).to_string()
}

pub fn string2time(s: &String) -> ddlog_std::Result<Time, String> {
    time_parse(s, &default_time_format.to_string())
}

impl FromRecord for Time {
    fn from_record(val: &record::Record) -> ::std::result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => {
                match (::chrono::NaiveTime::parse_from_str(s, &default_time_format.to_string())) {
                    Ok(t) => Ok(TimeWrapper { val: t }),
                    Err(e) => Err(format!("{}", e)),
                }
            }
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
    val: ::chrono::NaiveDate,
}
pub type Date = DateWrapper;

impl Default for DateWrapper {
    fn default() -> Self {
        let mid = ::chrono::NaiveDate::from_ymd(0, 1, 1);
        DateWrapper { val: mid }
    }
}

pub fn date_option_to_result(r: Option<::chrono::NaiveDate>) -> ddlog_std::Result<Date, String> {
    match (r) {
        Some(d) => ddlog_std::Result::Ok {
            res: DateWrapper { val: d },
        },
        None => ddlog_std::Result::Err {
            err: "Invalid date".to_string(),
        },
    }
}

pub fn try_from_ymd(year: &i32, month: &u8, day: &u8) -> ddlog_std::Result<Date, String> {
    date_option_to_result(::chrono::NaiveDate::from_ymd_opt(
        *year,
        *month as u32,
        *day as u32,
    ))
}

pub fn try_from_yo(year: &i32, ordinal: &u16) -> ddlog_std::Result<Date, String> {
    date_option_to_result(::chrono::NaiveDate::from_yo_opt(*year, *ordinal as u32))
}

fn convert_weekday_from_ddlog(weekday: &Weekday) -> ::chrono::Weekday {
    match (*weekday) {
        Weekday::Monday => ::chrono::Weekday::Mon,
        Weekday::Tuesday => ::chrono::Weekday::Tue,
        Weekday::Wednesday => ::chrono::Weekday::Wed,
        Weekday::Thursday => ::chrono::Weekday::Thu,
        Weekday::Friday => ::chrono::Weekday::Fri,
        Weekday::Saturday => ::chrono::Weekday::Sat,
        Weekday::Sunday => ::chrono::Weekday::Sun,
    }
}

fn convert_weekday_to_ddlog(weekday: ::chrono::Weekday) -> Weekday {
    match (weekday) {
        ::chrono::Weekday::Mon => Weekday::Monday,
        ::chrono::Weekday::Tue => Weekday::Tuesday,
        ::chrono::Weekday::Wed => Weekday::Wednesday,
        ::chrono::Weekday::Thu => Weekday::Thursday,
        ::chrono::Weekday::Fri => Weekday::Friday,
        ::chrono::Weekday::Sat => Weekday::Saturday,
        ::chrono::Weekday::Sun => Weekday::Sunday,
    }
}

pub fn try_from_iso_ywd(
    year: &i32,
    week: &u8,
    weekday: &Weekday,
) -> ddlog_std::Result<Date, String> {
    date_option_to_result(::chrono::NaiveDate::from_isoywd_opt(
        *year,
        *week as u32,
        convert_weekday_from_ddlog(weekday),
    ))
}

pub fn year(date: &Date) -> i32 {
    date.val.year()
}

pub fn month(date: &Date) -> u8 {
    date.val.month() as u8
}

pub fn day(date: &Date) -> u8 {
    date.val.day() as u8
}

pub fn ordinal(date: &Date) -> u16 {
    date.val.ordinal() as u16
}

pub fn week(date: &Date) -> u8 {
    date.val.iso_week().week() as u8
}

pub fn weekday(date: &Date) -> Weekday {
    convert_weekday_to_ddlog(date.val.weekday())
}

pub fn naivedate_to_timedate(date: ::chrono::NaiveDate) -> ::time::Date {
    ::time::Date::try_from_ymd(date.year(), date.month() as u8, date.day() as u8).unwrap()
}

pub fn timedate_to_naivedate(date: ::time::Date) -> ::chrono::NaiveDate {
    ::chrono::NaiveDate::from_ymd(date.year(), date.month().into(), date.day().into())
}

pub fn next_day(date: &Date) -> Date {
    DateWrapper {
        val: timedate_to_naivedate(naivedate_to_timedate(date.val).next_day()),
    }
}

pub fn previous_day(date: &Date) -> Date {
    DateWrapper {
        val: timedate_to_naivedate(naivedate_to_timedate(date.val).previous_day()),
    }
}

pub fn from_julian_day(julian_day: &i64) -> Date {
    DateWrapper {
        val: timedate_to_naivedate(::time::Date::from_julian_day(*julian_day)),
    }
}

pub fn julian_day(date: &Date) -> i64 {
    ::time::Date::julian_day(naivedate_to_timedate(date.val))
}

const default_date_format: &str = "%Y-%m-%d";

impl FromRecord for Date {
    fn from_record(val: &record::Record) -> ::std::result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => {
                match (::chrono::NaiveDate::parse_from_str(s, default_date_format)) {
                    Ok(d) => Ok(DateWrapper { val: d }),
                    Err(e) => Err(format!("{}", e)),
                }
            }
            _ => Err(String::from("Unexpected type")),
        }
    }
}

pub fn date_format(d: &Date, format: &String) -> ddlog_std::Result<String, String> {
    result_from_delayed_format(d.val.format(format), format)
}

pub fn date2string(d: &Date) -> String {
    // This should not panic since we know the format string is correct
    d.val.format(&default_date_format.to_string()).to_string()
}

pub fn date_parse(s: &String, format: &String) -> ddlog_std::Result<Date, String> {
    match (::chrono::NaiveDate::parse_from_str(s, format)) {
        Ok(d) => ddlog_std::Result::Ok {
            res: DateWrapper { val: d },
        },
        Err(e) => ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn string2date(s: &String) -> ddlog_std::Result<Date, String> {
    date_parse(s, &default_date_format.to_string())
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

pub fn datetime_parse(s: &String, format: &String) -> ddlog_std::Result<DateTime, String> {
    let prim = ::chrono::NaiveDateTime::parse_from_str(s, format);
    match (prim) {
        Ok(res) => {
            let dt = DateTime {
                date: DateWrapper { val: res.date() },
                time: TimeWrapper { val: res.time() },
            };
            ddlog_std::Result::Ok { res: dt }
        }
        Err(e) => ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn dateTime2string(d: &DateTime) -> String {
    let prim = ::chrono::NaiveDateTime::new((*d).date.val, (*d).time.val);
    // This should not panic since we know the format string is correct
    prim.format(defaultDateTimeFormat).to_string()
}

pub fn string2datetime(s: &String) -> ddlog_std::Result<DateTime, String> {
    datetime_parse(s, &String::from(defaultDateTimeFormat))
}

pub fn datetime_format(d: &DateTime, format: &String) -> ddlog_std::Result<String, String> {
    let dt = ::chrono::NaiveDateTime::new((*d).date.val, (*d).time.val);
    result_from_delayed_format(dt.format(format), format)
}

pub fn datetime_from_unix_timestamp(timestamp: &i64) -> DateTime {
    let odt = ::chrono::NaiveDateTime::from_timestamp(*timestamp, 0);
    DateTime {
        date: DateWrapper { val: odt.date() },
        time: TimeWrapper { val: odt.time() },
    }
}

//////////////////////////////////////////// Timezone ////////////////////////////////////

pub fn datetime_to_naivedatetime(dt: &DateTime) -> NaiveDateTime {
    NaiveDateTime::new(dt.date.val, dt.time.val)
}

pub fn naivedatetime_to_datetime(dt: &NaiveDateTime) -> DateTime {
    DateTime {
        date: DateWrapper { val: dt.date() },
        time: TimeWrapper { val: dt.time() },
    }
}

pub fn eastOffset(offset: &i32) -> FixedOffset {
    FixedOffset::east(*offset)
}

pub fn utc() -> FixedOffset {
    eastOffset(&0)
}

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct TzDateTime {
    // We only use FixedOffset, since it is the most general timezone offset
    val: ::chrono::DateTime<::chrono::FixedOffset>,
}

pub fn utc_timezone(dt: &DateTime) -> TzDateTime {
    TzDateTime {
        val: utc().from_utc_datetime(&datetime_to_naivedatetime(dt)),
    }
}

pub fn offset_timezone(dt: &DateTime, eastOffsetInSeconds: &i32) -> TzDateTime {
    TzDateTime {
        val: eastOffset(eastOffsetInSeconds).from_utc_datetime(&datetime_to_naivedatetime(dt)),
    }
}

pub fn to_rfc3339(dt: &TzDateTime) -> String {
    dt.val.to_rfc3339()
}

pub fn to_rfc2822(dt: &TzDateTime) -> String {
    dt.val.to_rfc2822()
}

pub fn tzDateTime2string(dt: &TzDateTime) -> String {
    to_rfc3339(dt)
}

pub fn tz_datetime_format(dt: &TzDateTime, format: &String) -> ddlog_std::Result<String, String> {
    result_from_delayed_format(dt.val.format(format), format)
}

pub fn tz_datetime_parse(s: &String, format: &String) -> ddlog_std::Result<TzDateTime, String> {
    match (::chrono::DateTime::parse_from_str(s, format)) {
        Ok(dt) => ddlog_std::Result::Ok {
            res: TzDateTime { val: dt },
        },
        Err(e) => ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn tz_datetime_parse_from_rfc3339(s: &String) -> ddlog_std::Result<TzDateTime, String> {
    match (::chrono::DateTime::parse_from_rfc3339(s)) {
        Ok(dt) => ddlog_std::Result::Ok {
            res: TzDateTime { val: dt },
        },
        Err(e) => ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn tz_datetime_parse_from_rfc2822(s: &String) -> ddlog_std::Result<TzDateTime, String> {
    match (::chrono::DateTime::parse_from_rfc2822(s)) {
        Ok(dt) => ddlog_std::Result::Ok {
            res: TzDateTime { val: dt },
        },
        Err(e) => ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

impl FromRecord for TzDateTime {
    fn from_record(val: &record::Record) -> ::std::result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => match (::chrono::DateTime::parse_from_rfc3339(s)) {
                Ok(dt) => Ok(TzDateTime { val: dt }),
                Err(e) => Err(format!("{}", e)),
            },
            _ => Err(String::from("Unexpected type")),
        }
    }
}

impl IntoRecord for TzDateTime {
    fn into_record(self) -> record::Record {
        record::Record::String(to_rfc3339(&self))
    }
}

impl Default for TzDateTime {
    fn default() -> Self {
        let defdt: DateTime = DateTime::default();
        utc_timezone(&defdt)
    }
}

impl record::Mutator<TzDateTime> for record::Record {
    fn mutate(&self, t: &mut TzDateTime) -> ::std::result::Result<(), String> {
        *t = TzDateTime::from_record(self)?;
        Ok(())
    }
}

pub fn time(td: &TzDateTime) -> Time {
    TimeWrapper { val: td.val.time() }
}

pub fn change_offset(dt: &TzDateTime, eastOffsetInSeconds: &i32) -> TzDateTime {
    TzDateTime {
        val: dt.val.with_timezone(&eastOffset(eastOffsetInSeconds)),
    }
}

pub fn utc_datetime(dt: &TzDateTime) -> DateTime {
    naivedatetime_to_datetime(&dt.val.naive_utc())
}
