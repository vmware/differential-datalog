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

use ordered_float::OrderedFloat;
use serde::de::DeserializeOwned;
use std::result::Result;

use ddlog_std::res2std;

pub fn _from_json_string<'de, T: serde::Deserialize<'de>>(
    json: &'de String,
) -> ddlog_std::Result<T, String> {
    res2std(serde_json::from_str::<'de>(&*json))
}

pub fn to_json_string<T: serde::Serialize>(x: &T) -> ddlog_std::Result<String, String> {
    res2std(serde_json::to_string(x))
}

pub fn _from_json_value<T: DeserializeOwned>(val: JsonValue) -> ddlog_std::Result<T, String> {
    res2std(serde_json::from_value(serde_json::value::Value::from(val)))
}

pub fn to_json_value<T: serde::Serialize>(x: T) -> ddlog_std::Result<JsonValue, String> {
    res2std(serde_json::to_value(x).map(JsonValue::from))
}

pub struct ValueWrapper(serde_json::value::Value);

impl serde::Serialize for ValueWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            serde_json::to_string(&self.0)
                .map_err(|e| serde::ser::Error::custom(e))?
                .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ValueWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            Ok(ValueWrapper(serde_json::Value::deserialize(deserializer)?))
        } else {
            Ok(ValueWrapper(
                serde_json::from_str::<serde_json::Value>(
                    String::deserialize(deserializer)?.as_ref(),
                )
                .map_err(|e| serde::de::Error::custom(e))?,
            ))
        }
    }
}

impl From<ValueWrapper> for JsonValue {
    fn from(x: ValueWrapper) -> Self {
        JsonValue::from(x.0)
    }
}

impl From<JsonValue> for ValueWrapper {
    fn from(x: JsonValue) -> Self {
        ValueWrapper(serde_json::Value::from(x))
    }
}

impl From<serde_json::value::Value> for JsonValue {
    fn from(x: serde_json::value::Value) -> Self {
        match x {
            serde_json::value::Value::Null => JsonValue::JsonNull,
            serde_json::value::Value::Bool(b) => JsonValue::JsonBool { b },
            serde_json::value::Value::Number(n) => JsonValue::JsonNumber {
                n: JsonNum::from(n),
            },
            serde_json::value::Value::String(s) => JsonValue::JsonString {
                s: internment::intern(s),
            },
            serde_json::value::Value::Array(a) => {
                let v: Vec<JsonValue> = a.into_iter().map(|v| JsonValue::from(v)).collect();
                JsonValue::JsonArray {
                    a: ddlog_std::Vec::from(v),
                }
            }
            serde_json::value::Value::Object(o) => JsonValue::JsonObject {
                o: o.into_iter()
                    .map(|(k, v)| (internment::intern(k), JsonValue::from(v)))
                    .collect(),
            },
        }
    }
}

impl From<JsonValue> for serde_json::value::Value {
    fn from(x: JsonValue) -> Self {
        match x {
            JsonValue::JsonNull => serde_json::value::Value::Null,
            JsonValue::JsonBool { b } => serde_json::value::Value::Bool(b),
            JsonValue::JsonNumber { n } => val_from_num(n),
            JsonValue::JsonString { s } => {
                serde_json::value::Value::String(internment::ival(&s).clone())
            }
            JsonValue::JsonArray { a } => serde_json::value::Value::Array(
                a.into_iter()
                    .map(|v| serde_json::value::Value::from(v))
                    .collect(),
            ),
            JsonValue::JsonObject { o } => serde_json::value::Value::Object(
                o.into_iter()
                    .map(|ddlog_std::tuple2(k, v)| {
                        (
                            internment::ival(&k).clone(),
                            serde_json::value::Value::from(v),
                        )
                    })
                    .collect(),
            ),
        }
    }
}

impl From<serde_json::Number> for JsonNum {
    fn from(n: serde_json::Number) -> Self {
        if n.is_u64() {
            JsonNum::JsonInt {
                i: n.as_u64().unwrap() as i128,
            }
        } else if n.is_i64() {
            JsonNum::JsonInt {
                i: n.as_i64().unwrap() as i128,
            }
        } else if n.is_f64() {
            JsonNum::JsonFloat {
                d: OrderedFloat::from(n.as_f64().unwrap()),
            }
        } else {
            panic!("JsonNum::from::<Number>(): unknown number format: '{}'", n)
        }
    }
}

fn val_from_num(n: JsonNum) -> serde_json::value::Value {
    match n {
        JsonNum::JsonInt { i } => {
            if i >= 0 {
                serde_json::value::Value::Number(serde_json::Number::from(i as u64))
            } else {
                serde_json::value::Value::Number(serde_json::Number::from(i as i64))
            }
        }
        JsonNum::JsonFloat { d } => serde_json::Number::from_f64(*d)
            .map(serde_json::value::Value::Number)
            .unwrap_or_else(|| serde_json::value::Value::Null),
    }
}

/* Some JSON schemas represent integers as strings.  This module serializes/deserializes
 * integers or any other types that implement `FromStr` and `ToString` traits to/from strings.
 * To use this module, add the following annotation to the DDlog field that needs to be
 * deserialized from a string:
 *
 * ```
 * typedef U64FromString = U64FromString {
 *   x: string,
 *   #[rust="serde(with=\"serde_string\")"]
 *   y: u64
 * }
 * ```
 */
pub mod serde_string {
    use serde::de;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;

    pub fn serialize<S, T>(x: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: std::string::ToString,
    {
        x.to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: ::std::fmt::Display,
    {
        T::from_str(&String::deserialize(deserializer)?).map_err(|e| de::Error::custom(e))
    }
}

impl<T: Serialize> Serialize for JsonWrapper<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            self.x.serialize(serializer)
        } else {
            serde_json::to_string(&self.x)
                .map_err(|e| serde::ser::Error::custom(e))?
                .serialize(serializer)
        }
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for JsonWrapper<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            Ok(JsonWrapper {
                x: T::deserialize(deserializer)?,
            })
        } else {
            Ok(JsonWrapper {
                x: serde_json::from_str::<T>(String::deserialize(deserializer)?.as_ref())
                    .map_err(|e| serde::de::Error::custom(e))?,
            })
        }
    }
}
