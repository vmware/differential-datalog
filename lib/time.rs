use std::fmt::Display;
use time::*;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct TimeWrapper {
    val: time::Time,
}
pub type time_time = TimeWrapper;

impl Default for TimeWrapper {
    fn default() -> Self {
        let mid = Time::midnight();
        TimeWrapper { val: mid }
    }
}

pub fn res2std_wrap<E: Display>(r: result::Result<time::Time, E>) -> std_Result<time_time, String> {
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

pub fn time_try_from_hms(h: &u8, m: &u8, s: &u8) -> std_Result<time_time, String> {
    res2std_wrap(Time::try_from_hms(*h, *m, *s))
}

pub fn time_try_from_hms_milli(h: &u8, m: &u8, s: &u8, ms: &u16) -> std_Result<time_time, String> {
    res2std_wrap(Time::try_from_hms_milli(*h, *m, *s, *ms))
}

pub fn time_try_from_hms_micro(h: &u8, m: &u8, s: &u8, mc: &u32) -> std_Result<time_time, String> {
    res2std_wrap(Time::try_from_hms_micro(*h, *m, *s, *mc))
}

pub fn time_try_from_hms_nano(h: &u8, m: &u8, s: &u8, mc: &u32) -> std_Result<time_time, String> {
    res2std_wrap(Time::try_from_hms_nano(*h, *m, *s, *mc))
}

pub fn time_hour(t: &time_time) -> u8 {
    Time::hour(t.val)
}

pub fn time_minute(t: &time_time) -> u8 {
    Time::minute(t.val)
}

pub fn time_second(t: &time_time) -> u8 {
    Time::second(t.val)
}

pub fn time_millisecond(t: &time_time) -> u16 {
    Time::millisecond(t.val)
}

pub fn time_microsecond(t: &time_time) -> u32 {
    Time::microsecond(t.val)
}

pub fn time_nanosecond(t: &time_time) -> u32 {
    Time::nanosecond(t.val)
}

pub fn time_time2string(t: &time_time) -> String {
    t.val.to_string()
}

pub fn time_midnight() -> time_time {
    TimeWrapper{
        val: Time::midnight(),
    }
}

pub fn time_parse(s: &String, format: &String) -> std_Result<time_time, String> {
    res2std_wrap(Time::parse(s, format))
}

impl FromRecord for time_time {
    fn from_record(val: &record::Record) -> result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => match (Time::parse(s, "%T")) {
                    Ok(s) => Ok(TimeWrapper{val: s}),
                    Err(e) => Err(format!("{}", e)),
            },
            _ => Err(String::from("Unexpected type")),
        }
    }
}

impl IntoRecord for time_time {
    fn into_record(self) -> record::Record {
        record::Record::String(self.val.to_string())
    }
}

impl record::Mutator<time_time> for record::Record {
    fn mutate(&self, t: &mut time_time) -> result::Result<(), String> {
        unimplemented!("time_time::mutator")
    }
}
