

fn split_ip_list(x: &String) -> Vec<String> {
    x.as_str().split(" ").map(|x| x.to_string()).collect()
}
