pub fn json_from_json_string<'de, T: serde::Deserialize<'de>>(
    json: &'de String,
) -> std_Result<T, String> {
    res2std(serde_json::from_str::<'de>(&*json))
}

pub fn json_to_json_string<T: serde::Serialize>(x: &T) -> std_Result<String, String> {
    res2std(serde_json::to_string(x))
}

impl From<serde_json::value::Value> for json_JsonValue {
    fn from(x: serde_json::value::Value) -> Self {
        match x {
            serde_json::value::Value::Null => json_JsonValue::json_JsonNull,
            serde_json::value::Value::Bool(b) => json_JsonValue::json_JsonBool { b },
            serde_json::value::Value::Number(n) => json_JsonValue::json_JsonNumber {
                n: json_JsonNum::from(n),
            },
            serde_json::value::Value::String(s) => json_JsonValue::json_JsonString { s },
            serde_json::value::Value::Array(a) => {
                let v: Vec<json_JsonValue> =
                    a.into_iter().map(|v| json_JsonValue::from(v)).collect();
                json_JsonValue::json_JsonArray {
                    a: std_Vec::from(v),
                }
            }
            serde_json::value::Value::Object(o) => json_JsonValue::json_JsonObject {
                o: o.into_iter()
                    .map(|(k, v)| (k, json_JsonValue::from(v)))
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
            json_JsonValue::json_JsonString { s } => serde_json::value::Value::String(s),
            json_JsonValue::json_JsonArray { a } => serde_json::value::Value::Array(
                a.into_iter()
                    .map(|v| serde_json::value::Value::from(v))
                    .collect(),
            ),
            json_JsonValue::json_JsonObject { o } => serde_json::value::Value::Object(
                o.into_iter()
                    .map(|(k, v)| (k, serde_json::value::Value::from(v)))
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
