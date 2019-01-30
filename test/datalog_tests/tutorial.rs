

pub fn split(x: &String, sep: &String) -> std_Vec<String> {
    std_Vec{x: x.as_str().split(sep).map(|x| x.to_string()).collect()}
}

pub fn string_slice(x: &String, from: &u64, to: &u64) -> String {
    x.as_str()[(*from as usize)..(*to as usize)].to_string()
}
