use ddlog_std::{Option as DDlogOption, Result as DDlogResult, Vec as DDlogVec};
use differential_datalog::record::{CollectionKind, FromRecord, IntoRecord, Mutator, Record};
use regex::{Error as RegexError, Regex as InnerRegex, RegexSet as InnerRegexSet};
use serde::{
    de::{Deserialize, Deserializer, Error},
    ser::{Serialize, Serializer},
};
use std::{
    cmp::Ordering,
    fmt::{Display, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    iter::{self, IntoIterator},
    ops::Deref,
};

#[derive(Debug, Clone)]
pub struct Regex {
    regex: InnerRegex,
}

impl Regex {
    pub fn new(regex: &str) -> Result<Self, RegexError> {
        Ok(Self {
            regex: InnerRegex::new(regex)?,
        })
    }
}

impl Display for Regex {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Display::fmt(&self.regex, f)
    }
}

impl Deref for Regex {
    type Target = InnerRegex;

    fn deref(&self) -> &Self::Target {
        &self.regex
    }
}

impl Default for Regex {
    fn default() -> Self {
        Self::new("").unwrap()
    }
}

impl PartialOrd for Regex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl Ord for Regex {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialEq for Regex {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for Regex {}

impl Hash for Regex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Serialize for Regex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Regex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let re = String::deserialize(deserializer)?;
        Regex::new(re.as_str()).map_err(Error::custom)
    }
}

impl IntoRecord for Regex {
    fn into_record(self) -> Record {
        Record::String(self.to_string())
    }
}

impl FromRecord for Regex {
    fn from_record(record: &Record) -> Result<Self, String> {
        match record {
            Record::String(regex) => Self::new(regex).map_err(|err| err.to_string()),
            error => Err(format!("not a valid regex: {:?}", error)),
        }
    }
}

impl Mutator<Regex> for Record {
    fn mutate(&self, regex: &mut Regex) -> Result<(), String> {
        *regex = Regex::from_record(self)?;
        Ok(())
    }
}

/// If the provided string is not a valid regex, this function will return
/// a regex that will not match any string
pub fn regex(regex: &str) -> Regex {
    Regex::new(regex).unwrap_or_else(|_| Regex::new(r"a^").unwrap())
}

/// Attempts to create a regex from the given string, returning an error
/// if it is invalid
pub fn try_regex(regex: &str) -> DDlogResult<Regex, String> {
    ddlog_std::res2std(Regex::new(regex))
}

/// Returns true if the regex matches the given text
pub fn regex_match(regex: &Regex, text: &str) -> bool {
    regex.is_match(&text)
}

/// Gets the first match of a regex
pub fn regex_first_match(regex: &Regex, text: &str) -> DDlogOption<String> {
    regex
        .find(&text)
        .map(|found| found.as_str().to_string())
        .into()
}

/// Collects all matches from a regex
pub fn regex_all_matches(regex: &Regex, text: &str) -> DDlogVec<String> {
    regex
        .find_iter(&text)
        .map(|found| found.as_str().to_owned())
        .collect()
}

#[derive(Debug, Clone)]
pub struct RegexSet {
    set: InnerRegexSet,
}

impl RegexSet {
    pub fn new<I, S>(patterns: I) -> Result<Self, RegexError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Ok(Self {
            set: InnerRegexSet::new(patterns)?,
        })
    }
}

impl Deref for RegexSet {
    type Target = InnerRegexSet;

    fn deref(&self) -> &Self::Target {
        &self.set
    }
}

impl Default for RegexSet {
    fn default() -> Self {
        Self::new(iter::empty::<&str>()).unwrap()
    }
}

impl PartialOrd for RegexSet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.patterns().partial_cmp(other.patterns())
    }
}

impl Ord for RegexSet {
    fn cmp(&self, other: &Self) -> Ordering {
        self.patterns().cmp(other.patterns())
    }
}

impl PartialEq for RegexSet {
    fn eq(&self, other: &Self) -> bool {
        self.patterns().eq(other.patterns())
    }
}

impl Eq for RegexSet {}

impl Hash for RegexSet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.patterns().hash(state);
    }
}

impl Serialize for RegexSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.patterns().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RegexSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let regex = <Vec<String>>::deserialize(deserializer)?;
        RegexSet::new(regex).map_err(Error::custom)
    }
}

impl IntoRecord for RegexSet {
    fn into_record(self) -> Record {
        let patterns = self
            .patterns()
            .iter()
            .cloned()
            .map(IntoRecord::into_record)
            .collect();

        Record::Array(CollectionKind::Vector, patterns)
    }
}

impl FromRecord for RegexSet {
    fn from_record(record: &Record) -> Result<Self, String> {
        match record {
            Record::Array(CollectionKind::Vector, patterns) => Self::new(
                patterns
                    .iter()
                    .map(FromRecord::from_record)
                    .collect::<Result<Vec<String>, _>>()?,
            )
            .map_err(|err| err.to_string()),

            error => Err(format!("not a valid regex set: {:?}", error)),
        }
    }
}

impl Mutator<RegexSet> for Record {
    fn mutate(&self, regex: &mut RegexSet) -> Result<(), String> {
        *regex = RegexSet::from_record(self)?;
        Ok(())
    }
}

/// If any of the given patterns are not a valid regex this function will return
/// an empty set
pub fn regex_set(patterns: &[String]) -> RegexSet {
    RegexSet::new(patterns).unwrap_or_else(|_| RegexSet::new(iter::empty::<&str>()).unwrap())
}

/// Attempts to create a regex set from the given patterns, returning an error
/// if it is invalid
pub fn try_regex_set(patterns: &[String]) -> DDlogResult<RegexSet, String> {
    ddlog_std::res2std(RegexSet::new(patterns))
}

/// Returns true if any regex in the set matches the given text
pub fn regex_set_match(regex: &RegexSet, text: &str) -> bool {
    regex.is_match(&text)
}
