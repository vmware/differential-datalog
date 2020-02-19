pub fn json_from_json_string<'de, T: serde::Deserialize<'de>>(
    json: &'de String,
) -> std_Result<T, String> {
    res2std(serde_json::from_str::<'de>(&*json))
}

pub fn json_to_json_string<T: serde::Serialize>(x: &T) -> std_Result<String, String> {
    res2std(serde_json::to_string(x))
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScalarValueInner {
    ValString(String),
    ValBool(bool),
    ValInt(i64),
}

impl From<ScalarValueInner> for json_JsonScalarValue {
    fn from(x: ScalarValueInner) -> Self {
        match x {
            ScalarValueInner::ValString(s) => json_JsonScalarValue::json_ValString { s },
            ScalarValueInner::ValBool(b) => json_JsonScalarValue::json_ValBool { b },
            ScalarValueInner::ValInt(i) => json_JsonScalarValue::json_ValInt { i },
        }
    }
}

impl From<json_JsonScalarValue> for ScalarValueInner {
    fn from(x: json_JsonScalarValue) -> Self {
        match x {
            json_JsonScalarValue::json_ValString { s } => ScalarValueInner::ValString(s),
            json_JsonScalarValue::json_ValBool { b } => ScalarValueInner::ValBool(b),
            json_JsonScalarValue::json_ValInt { i } => ScalarValueInner::ValInt(i),
        }
    }
}
