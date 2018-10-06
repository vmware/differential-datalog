

fn split(x: &String, sep: &String) -> std_Vec<arcval::DDString> {
    std_Vec{x: x.as_str().split(sep).map(|x| arcval::DDString::from(x.to_string())).collect()}
}

fn string_slice(x: &String, from: &u64, to: &u64) -> arcval::DDString {
    arcval::DDString::from(x.as_str()[(*from as usize)..(*to as usize)].to_string())
}
