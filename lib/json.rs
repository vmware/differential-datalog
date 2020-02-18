pub fn json_from_json_string<'de, T: serde::Deserialize<'de>>(
    json: &'de String,
) -> std_Result<T, String> {
    res2std(serde_json::from_str::<'de>(&*json))
}

pub fn json_to_json_string<T: serde::Serialize>(x: &T) -> std_Result<String, String> {
    res2std(serde_json::to_string(x))
}
