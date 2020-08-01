pub fn json_from_json_string<'de, T: serde::Deserialize<'de>>(
    json: &'de String,
) -> std_Result<T, String> {
    res2std(serde_json::from_str::<'de>(&*json))
}

pub fn json_to_json_string<T: serde::Serialize>(x: &T) -> std_Result<String, String> {
    res2std(serde_json::to_string(x))
}

pub fn json_from_json_value<T: DeserializeOwned>(
    json_val: &json_JsonValue,
) -> std_Result<T, String> {
    res2std(serde_json::from_value(serde_json::value::Value::from(
        json_val.clone(),
    )))
}

pub fn json_to_json_value<T: serde::Serialize>(x: &T) -> std_Result<json_JsonValue, String> {
    res2std(serde_json::to_value(x.clone()).map(json_JsonValue::from))
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

impl From<ValueWrapper> for json_JsonValue {
    fn from(x: ValueWrapper) -> Self {
        json_JsonValue::from(x.0)
    }
}

impl From<json_JsonValue> for ValueWrapper {
    fn from(x: json_JsonValue) -> Self {
        ValueWrapper(serde_json::Value::from(x))
    }
}

impl From<serde_json::value::Value> for json_JsonValue {
    fn from(x: serde_json::value::Value) -> Self {
        match x {
            serde_json::value::Value::Null => json_JsonValue::json_JsonNull,
            serde_json::value::Value::Bool(b) => json_JsonValue::json_JsonBool { b },
            serde_json::value::Value::Number(n) => json_JsonValue::json_JsonNumber {
                n: json_JsonNum::from(n),
            },
            serde_json::value::Value::String(s) => json_JsonValue::json_JsonString {
                s: internment_intern(&s),
            },
            serde_json::value::Value::Array(a) => {
                let v: Vec<json_JsonValue> =
                    a.into_iter().map(|v| json_JsonValue::from(v)).collect();
                json_JsonValue::json_JsonArray {
                    a: std_Vec::from(v),
                }
            }
            serde_json::value::Value::Object(o) => json_JsonValue::json_JsonObject {
                o: o.into_iter()
                    .map(|(k, v)| (internment_intern(&k), json_JsonValue::from(v)))
                    .collect(),
            },
        }
    }
}

impl From<json_JsonValue> for serde_json::value::Value {
    fn from(x: json_JsonValue) -> Self {
        match x {
            json_JsonValue::json_JsonNull => serde_json::value::Value::Null,
            json_JsonValue::json_JsonBool { b } => serde_json::value::Value::Bool(b),
            json_JsonValue::json_JsonNumber { n } => json_val_from_num(n),
            json_JsonValue::json_JsonString { s } => {
                serde_json::value::Value::String(internment_ival(&s).clone())
            }
            json_JsonValue::json_JsonArray { a } => serde_json::value::Value::Array(
                a.into_iter()
                    .map(|v| serde_json::value::Value::from(v))
                    .collect(),
            ),
            json_JsonValue::json_JsonObject { o } => serde_json::value::Value::Object(
                o.into_iter()
                    .map(|(k, v)| {
                        (
                            internment_ival(&k).clone(),
                            serde_json::value::Value::from(v),
                        )
                    })
                    .collect(),
            ),
        }
    }
}

impl From<serde_json::Number> for json_JsonNum {
    fn from(n: serde_json::Number) -> Self {
        if n.is_u64() {
            json_JsonNum::json_JsonInt {
                i: n.as_u64().unwrap() as i128,
            }
        } else if n.is_i64() {
            json_JsonNum::json_JsonInt {
                i: n.as_i64().unwrap() as i128,
            }
        } else if n.is_f64() {
            json_JsonNum::json_JsonFloat {
                d: OrderedFloat::from(n.as_f64().unwrap()),
            }
        } else {
            panic!("JsonNum::from::<Number>(): unknown number format: '{}'", n)
        }
    }
}

fn json_val_from_num(n: json_JsonNum) -> serde_json::value::Value {
    match n {
        json_JsonNum::json_JsonInt { i } => {
            if i >= 0 {
                serde_json::value::Value::Number(serde_json::Number::from(i as u64))
            } else {
                serde_json::value::Value::Number(serde_json::Number::from(i as i64))
            }
        }
        json_JsonNum::json_JsonFloat { d } => serde_json::Number::from_f64(*d)
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
        <T as std::str::FromStr>::Err: std::fmt::Display,
    {
        T::from_str(&String::deserialize(deserializer)?).map_err(|e| de::Error::custom(e))
    }
}

impl<T: Serialize> Serialize for json_JsonWrapper<T> {
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

impl<'de, T: DeserializeOwned> Deserialize<'de> for json_JsonWrapper<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            Ok(json_JsonWrapper {
                x: T::deserialize(deserializer)?,
            })
        } else {
            Ok(json_JsonWrapper {
                x: serde_json::from_str::<T>(String::deserialize(deserializer)?.as_ref())
                    .map_err(|e| serde::de::Error::custom(e))?,
            })
        }
    }
}
