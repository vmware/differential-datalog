#[derive(Clone)]
pub struct Regex {
    re: ::regex::Regex,
}

impl Regex {
    pub fn new(re: &str) -> Result<Self, ::regex::Error> {
        Ok(Regex {
            re: ::regex::Regex::new(re)?,
        })
    }
}

impl std::ops::Deref for Regex {
    type Target = ::regex::Regex;

    fn deref(&self) -> &::regex::Regex {
        &self.re
    }
}

impl Default for Regex {
    fn default() -> Self {
        Self::new("").unwrap()
    }
}

impl PartialOrd for Regex {
    fn partial_cmp(&self, other: &Regex) -> Option<std::cmp::Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl Ord for Regex {
    fn cmp(&self, other: &Regex) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialEq for Regex {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for Regex {}

impl std::hash::Hash for Regex {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Serialize for Regex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Regex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let re = String::deserialize(deserializer)?;
        Regex::new(re.as_str()).map_err(|e| serde::de::Error::custom(e))
    }
}

pub fn regex(regex: &String) -> Regex {
    Regex::new(regex.as_str()).unwrap_or_else(|_|/* Reject everything. */
                        Regex::new(r"a^").unwrap())
}

pub fn regex_checked(regex: &String) -> crate::ddlog_std::Result<Regex, String> {
    crate::ddlog_std::res2std(Regex::new(regex.as_str()))
}

pub fn regex_match(regex: &Regex, s: &String) -> bool {
    regex.is_match(&s.as_str())
}

pub fn regex_first_match(regex: &Regex, s: &String) -> crate::ddlog_std::Option<String> {
    crate::ddlog_std::Option::from(regex.find(&s.as_str()).map(|m| m.as_str().to_string()))
}

pub fn regex_all_matches(regex: &Regex, s: &String) -> crate::ddlog_std::Vec<String> {
    let v: Vec<_> = regex
        .find_iter(&s.as_str())
        .map(|m| m.as_str().to_string())
        .collect();
    crate::ddlog_std::Vec::from(v)
}
