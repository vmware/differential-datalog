

fn split_ip_list(x: &String) -> Vec<String> {
    x.as_str().split(" ").map(|x| x.to_string()).collect()
}

//fn string_slice(x: &String)
fn string_slice(x: &String, from: &u64, to: &u64) -> String {
    x.as_str()[(*from as usize)..(*to as usize)].to_string()
}
