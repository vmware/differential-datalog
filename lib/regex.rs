extern crate regex;

pub fn regex_regex_match(regex: &String, s: &String) -> bool {
    match regex::Regex::new(regex.as_str()) {
        Result::Err(_) => false,
        Result::Ok(re) => re.is_match(&s.as_str()),
    }
}

pub fn regex_regex_first_match(regex: &String, s: &String) -> std_Option<String> {
    match regex::Regex::new(regex.as_str()) {
        Result::Err(_) => std_Option::std_None,
        Result::Ok(re) => std_Option::from(re.find(&s.as_str()).map(|m| m.as_str().to_string())),
    }
}

pub fn regex_regex_all_matches(regex: &String, s: &String) -> std_Vec<String> {
    match regex::Regex::new(regex.as_str()) {
        Result::Err(_) => std_Vec::new(),
        Result::Ok(re) => {
            let v: Vec<_> = re
                .find_iter(&s.as_str())
                .map(|m| m.as_str().to_string())
                .collect();
            std_Vec::from(v)
        }
    }
}
