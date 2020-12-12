use arc_interner::ArcIntern;
use ddlog_std::Vec as DDlogVec;
use differential_datalog::record::{self, Record};
use serde::{de::Deserializer, ser::Serializer};
use std::{
    cmp::{self, Ordering},
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    hash::Hash,
};

/// An atomically reference counted handle to an interned value
///
/// The `PartialOrd` and `Ord` implementations for this type do not
/// compare the underlying values but instead compare the pointers
/// to them. Do not rely on the `PartialOrd` and `Ord` implementations
/// for persistent storage, determinism or for the ordering of the
/// underlying values, as pointers will change from run to run.
#[derive(Default, Eq, PartialEq, Clone, Hash)]
pub struct Intern<A>
where
    A: Eq + Send + Sync + Hash + 'static,
{
    interned: ArcIntern<A>,
}

impl<T> Intern<T>
where
    T: Eq + Hash + Send + Sync + 'static,
{
    /// Create a new interned value
    pub fn new(value: T) -> Self {
        Intern {
            interned: ArcIntern::new(value),
        }
    }

    /// Get the current interned item's pointer as a `usize`
    fn as_usize(&self) -> usize {
        self.as_ref() as *const T as usize
    }
}

impl<T> PartialEq<T> for Intern<T>
where
    T: Eq + Send + Sync + Hash + 'static,
{
    fn eq(&self, other: &T) -> bool {
        self.as_ref().eq(&other)
    }
}

/// Order the interned values by their pointers
impl<T> PartialOrd for Intern<T>
where
    T: Eq + Send + Sync + Hash + 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_usize().partial_cmp(&other.as_usize())
    }
}

/// Order the interned values by their pointers
impl<T> Ord for Intern<T>
where
    T: Eq + Send + Sync + Hash + 'static,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_usize().cmp(&other.as_usize())
    }
}

impl<T> Deref for Intern<T>
where
    T: Eq + Send + Sync + Hash + 'static,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.interned.deref()
    }
}

impl<T> AsRef<T> for Intern<T>
where
    T: Eq + Hash + Send + Sync + 'static,
{
    fn as_ref(&self) -> &T {
        self.interned.as_ref()
    }
}

impl<T> From<T> for Intern<T>
where
    T: Eq + Hash + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> From<&[T]> for Intern<Vec<T>>
where
    T: Eq + Hash + Send + Sync + Clone + 'static,
{
    fn from(slice: &[T]) -> Self {
        Self::new(slice.to_vec())
    }
}

impl From<&str> for Intern<String> {
    fn from(string: &str) -> Self {
        Self::new(string.to_owned())
    }
}

impl<T> Display for Intern<T>
where
    T: Display + Eq + Hash + Send + Sync,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Display::fmt(self.as_ref(), f)
    }
}

impl<T> Debug for Intern<T>
where
    T: Debug + Eq + Hash + Send + Sync,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Debug::fmt(self.as_ref(), f)
    }
}

impl<T> Serialize for Intern<T>
where
    T: Serialize + Eq + Hash + Send + Sync,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_ref().serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Intern<T>
where
    T: Deserialize<'de> + Eq + Hash + Send + Sync + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Intern::new)
    }
}

impl<T> FromRecord for Intern<T>
where
    T: FromRecord + Eq + Hash + Send + Sync + 'static,
{
    fn from_record(val: &Record) -> Result<Self, String> {
        T::from_record(val).map(Intern::new)
    }
}

impl<T> IntoRecord for Intern<T>
where
    T: IntoRecord + Eq + Hash + Send + Sync + Clone,
{
    fn into_record(self) -> Record {
        ival(&self).clone().into_record()
    }
}

impl<T> Mutator<Intern<T>> for Record
where
    T: Clone + Eq + Send + Sync + Hash,
    Record: Mutator<T>,
{
    fn mutate(&self, value: &mut Intern<T>) -> Result<(), String> {
        let mut mutated = ival(value).clone();
        self.mutate(&mut mutated)?;
        *value = intern(&mutated);

        Ok(())
    }
}

/// Create a new interned value
pub fn intern<T>(value: &T) -> Intern<T>
where
    T: Eq + Hash + Send + Sync + Clone + 'static,
{
    Intern::new(value.clone())
}

/// Get the inner value of an interned value
pub fn ival<T>(value: &Intern<T>) -> &T
where
    T: Eq + Hash + Send + Sync + Clone,
{
    value.interned.as_ref()
}

/// Join interned strings with a separator
pub fn istring_join(strings: &DDlogVec<istring>, separator: &String) -> String {
    strings
        .vec
        .iter()
        .map(|string| string.as_ref())
        .cloned()
        .collect::<Vec<String>>()
        .join(separator.as_str())
}

/// Split an interned string by a separator
pub fn istring_split(string: &istring, separator: &String) -> DDlogVec<String> {
    DDlogVec {
        vec: string
            .as_ref()
            .split(separator)
            .map(|string| string.to_owned())
            .collect(),
    }
}

/// Returns true if the interned string contains the given pattern
pub fn istring_contains(interned: &istring, pattern: &String) -> bool {
    interned.as_ref().contains(pattern.as_str())
}

pub fn istring_substr(string: &istring, start: &std_usize, end: &std_usize) -> String {
    let len = string.as_ref().len();
    let from = cmp::min(*start as usize, len);
    let to = cmp::max(from, cmp::min(*end as usize, len));

    string.as_ref()[from..to].to_string()
}

pub fn istring_replace(string: &istring, from: &String, to: &String) -> String {
    string.as_ref().replace(from, to)
}

pub fn istring_starts_with(string: &istring, prefix: &String) -> bool {
    string.as_ref().starts_with(prefix)
}

pub fn istring_ends_with(string: &istring, suffix: &String) -> bool {
    string.as_ref().ends_with(suffix)
}

pub fn istring_trim(string: &istring) -> String {
    string.as_ref().trim().to_string()
}

pub fn istring_len(string: &istring) -> std_usize {
    string.as_ref().len() as std_usize
}

pub fn istring_to_bytes(string: &istring) -> DDlogVec<u8> {
    DDlogVec::from(string.as_ref().as_bytes())
}

pub fn istring_to_lowercase(string: &istring) -> String {
    string.as_ref().to_lowercase()
}

pub fn istring_to_uppercase(string: &istring) -> String {
    string.as_ref().to_uppercase()
}

pub fn istring_reverse(string: &istring) -> String {
    string.as_ref().chars().rev().collect()
}
