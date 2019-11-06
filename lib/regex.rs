extern crate regex;

pub fn regex_regex_match(regex: &String, s: &String) -> bool {
    match regex::Regex::new(regex.as_str()) {
        Result::Err(_) => false,
        Result::Ok(re) => re.is_match(&s.as_str()),
    }
}
