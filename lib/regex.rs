extern crate regex;

#[derive(Clone)]
pub struct regex_Regex {
    re: regex::Regex,
}

impl regex_Regex {
    pub fn new(re: &str) -> Result<Self, regex::Error> {
        Ok(regex_Regex {
            re: regex::Regex::new(re)?,
        })
    }
}

impl Deref for regex_Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &regex::Regex {
        &self.re
    }
}

impl PartialOrd for regex_Regex {
    fn partial_cmp(&self, other: &regex_Regex) -> Option<std::cmp::Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl Ord for regex_Regex {
    fn cmp(&self, other: &regex_Regex) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialEq for regex_Regex {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for regex_Regex {}

impl Hash for regex_Regex {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Serialize for regex_Regex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for regex_Regex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let re = String::deserialize(deserializer)?;
        regex_Regex::new(re.as_str()).map_err(|e| serde::de::Error::custom(e))
    }
}

pub fn regex_regex(regex: &String) -> regex_Regex {
    regex_Regex::new(regex.as_str()).unwrap_or_else(|_|/* Reject everything. */
                        regex_Regex::new(r"a^").unwrap())
}

pub fn regex_regex_checked(regex: &String) -> std_Result<regex_Regex, String> {
    res2std(regex_Regex::new(regex.as_str()))
}

pub fn regex_regex_match(regex: &regex_Regex, s: &String) -> bool {
    regex.is_match(&s.as_str())
}

pub fn regex_regex_first_match(regex: &regex_Regex, s: &String) -> std_Option<String> {
    std_Option::from(regex.find(&s.as_str()).map(|m| m.as_str().to_string()))
}

pub fn regex_regex_all_matches(regex: &regex_Regex, s: &String) -> std_Vec<String> {
    let v: Vec<_> = regex
        .find_iter(&s.as_str())
        .map(|m| m.as_str().to_string())
        .collect();
    std_Vec::from(v)
}
