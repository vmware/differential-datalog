/*
Copyright (c) 2021 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use ddlog_std::{hash32, Vec as DDlogVec};
use differential_datalog::record::{self, Record};
use internment::ArcIntern;
use serde::{de::Deserializer, ser::Serializer};
use std::{
    cmp::{self, Ordering},
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
};

/// An atomically reference counted handle to an interned value.
/// In addition to memory deduplication, this type is optimized for fast comparison.
/// To this end, we store a 32-bit hash along with the interned value and use this hash for
/// comparison, only falling back to by-value comparison in case of a hash collision.
#[derive(Default, Eq, PartialEq, Clone)]
pub struct Intern<A>
where
    A: Eq + Send + Sync + Hash + 'static,
{
    interned: ArcIntern<(u32, A)>,
}

impl<T: Hash + Eq + Send + Sync + 'static> Hash for Intern<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.as_ref().hash(state)
    }
}

impl<T> Intern<T>
where
    T: Eq + Hash + Send + Sync + 'static,
{
    /// Create a new interned value.
    pub fn new(value: T) -> Self {
        // Hash the value.  Note: this is technically redundant,
        // as `ArcIntern` hashes the value internally, but we
        // cannot easily access that hash value.
        let hash = hash32(&value);
        Intern {
            interned: ArcIntern::new((hash as u32, value)),
        }
    }

    /// Get the current interned item's pointer as a `usize`
    fn as_usize(&self) -> usize {
        self.as_ref() as *const T as usize
    }
}

/// Order the interned values:
/// - Start with comparing pointers.  The two values are the same if and only if the pointers are
/// the same.
/// - Otherwise, compare their 32-bit hashes and order them based on hash values.
/// - In the extremely rare case where a hash collision occurs, compare the actual values.
impl<T> Ord for Intern<T>
where
    T: Eq + Ord + Send + Sync + Hash + 'static,
{
    fn cmp(&self, other: &Self) -> Ordering {
        if self.as_usize() == other.as_usize() {
            return Ordering::Equal;
        } else {
            match self.interned.as_ref().0.cmp(&other.interned.as_ref().0) {
                Ordering::Equal => self.as_ref().cmp(other.as_ref()),
                ord => ord,
            }
        }
    }
}

impl<T> PartialOrd for Intern<T>
where
    T: Eq + Ord + Send + Sync + Hash + 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Deref for Intern<T>
where
    T: Eq + Send + Sync + Hash + 'static,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.interned.deref().1
    }
}

impl<T> AsRef<T> for Intern<T>
where
    T: Eq + Hash + Send + Sync + 'static,
{
    fn as_ref(&self) -> &T {
        &self.interned.as_ref().1
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
    value.as_ref()
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
