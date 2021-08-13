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

use differential_datalog::record::*;
use serde;
use std::cmp;
use std::collections;
use std::fmt;
use std::iter;
use std::iter::FromIterator;
use std::ops::BitOr;
use std::vec;
pub use tinyset::u64set;

use ddlog_std::option2std;

#[derive(Eq, Clone, Hash, PartialEq)]
pub struct Set64<T: u64set::Fits64> {
    pub x: u64set::Set64<T>,
}

/* This is needed so we can support for-loops over `Set64`'s.
 *
 * IMPORTANT: We iterate over Set64 by converting it into a sorted vector.
 * This can be costly, but is necessary to make sure that all `Set64`
 * operations are deterministic.
 */
pub struct Set64Iter<X: u64set::Fits64 + Ord> {
    iter: vec::IntoIter<X>,
}

impl<X: u64set::Fits64 + Ord> Set64Iter<X> {
    pub fn new(set: &Set64<X>) -> Set64Iter<X> {
        let mut v: Vec<_> = set.x.iter().collect();
        v.sort();
        Set64Iter {
            iter: v.into_iter(),
        }
    }
}

impl<X: u64set::Fits64 + Ord> Iterator for Set64Iter<X> {
    type Item = X;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<T: u64set::Fits64 + Ord> Set64<T> {
    pub fn iter(&self) -> Set64Iter<T> {
        Set64Iter::new(self)
    }
}

impl<T: u64set::Fits64> Set64<T> {
    /* In cases when order really, definitely, 100% does not matter,
     * a more efficient iterator can be used.
     */
    pub fn unsorted_iter(&self) -> u64set::Iter64<T> {
        self.x.iter()
    }
}

impl<T: u64set::Fits64 + Ord> Ord for Set64<T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let size_cmp = self.x.len().cmp(&other.x.len());
        if size_cmp != cmp::Ordering::Equal {
            size_cmp
        } else {
            self.iter().cmp(other.iter())
        }
    }
}

impl<T: u64set::Fits64 + Ord> PartialOrd for Set64<T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: u64set::Fits64 + Ord> Default for Set64<T> {
    fn default() -> Self {
        Set64 {
            x: u64set::Set64::default(),
        }
    }
}

impl<T: u64set::Fits64 + serde::Serialize + Ord> serde::Serialize for Set64<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de, T: u64set::Fits64 + serde::Deserialize<'de> + Ord> serde::Deserialize<'de> for Set64<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        panic!("Set64::deserialize is not implemented")
    }
}

impl<T: u64set::Fits64> Set64<T> {
    pub fn new() -> Self {
        Set64 {
            x: u64set::Set64::new(),
        }
    }
    pub fn insert(&mut self, v: T) {
        self.x.insert(v);
    }
    pub fn remove(&mut self, v: &T) -> bool {
        self.x.remove(v)
    }
}

impl<T: FromRecord + ::serde::de::DeserializeOwned + u64set::Fits64> FromRecordInner for Set64<T> {
    fn from_record_inner(val: &Record) -> Result<Self, String> {
        vec::Vec::from_record(val).map(|v| Set64 {
            x: u64set::Set64::from_iter(v),
        })
    }
}

impl<T: IntoRecord + u64set::Fits64 + Ord> IntoRecord for Set64<T> {
    fn into_record(self) -> Record {
        Record::Array(
            CollectionKind::Set,
            self.into_iter().map(|x| x.into_record()).collect(),
        )
    }
}

impl<T: FromRecord + ::serde::de::DeserializeOwned + u64set::Fits64 + Ord> Mutator<Set64<T>>
    for Record
{
    fn mutate(&self, set: &mut Set64<T>) -> Result<(), String> {
        let upd = <Set64<T>>::from_record(self)?;
        for v in upd.into_iter() {
            if !set.remove(&v) {
                set.insert(v);
            }
        }
        Ok(())
    }
}

impl<T: u64set::Fits64 + Ord> iter::IntoIterator for &Set64<T> {
    type Item = T;
    type IntoIter = Set64Iter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: u64set::Fits64> FromIterator<T> for Set64<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: iter::IntoIterator<Item = T>,
    {
        Set64 {
            x: u64set::Set64::from_iter(iter),
        }
    }
}

impl<T: fmt::Display + u64set::Fits64 + Ord> fmt::Display for Set64<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, v) in self.iter().enumerate() {
            formatter.write_fmt(format_args!("{}", v))?;
            if i < len - 1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

impl<T: fmt::Debug + u64set::Fits64 + Ord> fmt::Debug for Set64<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, v) in self.iter().enumerate() {
            formatter.write_fmt(format_args!("{:?}", v))?;
            if i < len - 1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

pub fn size<X: u64set::Fits64>(s: &Set64<X>) -> u64 {
    s.x.len() as u64
}

pub fn empty<X: u64set::Fits64>() -> Set64<X> {
    Set64::new()
}

pub fn singleton<X: u64set::Fits64 + Clone>(v: &X) -> Set64<X> {
    let mut s = Set64::new();
    s.insert(v.clone());
    s
}

pub fn insert<X: u64set::Fits64 + Clone>(s: &mut Set64<X>, v: &X) {
    s.x.insert((*v).clone());
}

pub fn insert_imm<X: u64set::Fits64>(mut s: Set64<X>, v: X) -> Set64<X> {
    s.insert(v);
    s
}

pub fn contains<X: u64set::Fits64>(s: &Set64<X>, v: &X) -> bool {
    s.x.contains(*v)
}

pub fn is_empty<X: u64set::Fits64>(s: &Set64<X>) -> bool {
    s.x.len() == 0
}

pub fn nth<X: u64set::Fits64 + Ord + Clone>(s: &Set64<X>, n: &u64) -> ddlog_std::Option<X> {
    option2std(s.iter().nth(*n as usize))
}

pub fn set2vec<X: u64set::Fits64 + Ord + Clone>(s: &Set64<X>) -> ddlog_std::Vec<X> {
    let mut v: Vec<_> = s.x.iter().collect();
    v.sort();
    ddlog_std::Vec::from(v)
}

pub fn union<X: u64set::Fits64>(mut s1: Set64<X>, s2: &Set64<X>) -> Set64<X> {
    Set64 {
        x: s1.x.bitor(&s2.x),
    }
}

pub fn unions<X: u64set::Fits64 + Clone>(sets: &ddlog_std::Vec<Set64<X>>) -> Set64<X> {
    let mut s = u64set::Set64::new();
    for si in sets.iter() {
        for v in si.unsorted_iter() {
            s.insert(v);
        }
    }
    Set64 { x: s }
}

pub fn intersection<X: u64set::Fits64 + Clone>(s1: &Set64<X>, s2: &Set64<X>) -> Set64<X> {
    let mut s = u64set::Set64::new();
    for v in s1.unsorted_iter() {
        if s2.x.contains(v) {
            s.insert(v);
        }
    }
    Set64 { x: s }
}

pub fn difference<X: u64set::Fits64 + Clone>(s1: &Set64<X>, s2: &Set64<X>) -> Set64<X> {
    Set64 {
        x: std::ops::Sub::sub(&s1.x, &s2.x),
    }
}

pub fn group_to_set<K, V: u64set::Fits64>(g: &ddlog_std::Group<K, V>) -> Set64<V> {
    let mut res = Set64::new();
    for ref v in g.val_iter() {
        insert(&mut res, v);
    }
    res
}

pub fn group_set_unions<K, V: u64set::Fits64 + Clone>(
    g: &ddlog_std::Group<K, Set64<V>>,
) -> Set64<V> {
    let mut res = u64set::Set64::new();
    for gr in g.val_iter() {
        for v in gr.unsorted_iter() {
            res.insert(v.clone());
        }
        //`extend` should be more efficient, but is not yet implemented in the tinyset library
        //res.extend(&mut g.ith(i).x.iter());
    }
    Set64 { x: res }
}

pub fn group_setref_unions<K, V: u64set::Fits64 + Ord + Clone>(
    g: &ddlog_std::Group<K, ddlog_std::Ref<Set64<V>>>,
) -> ddlog_std::Ref<Set64<V>> {
    if ddlog_std::group_count(g) == 1 {
        ddlog_std::group_first(g)
    } else {
        let mut res = ddlog_std::ref_new(Set64 {
            x: u64set::Set64::new(),
        });
        {
            let rres = ddlog_std::Ref::get_mut(&mut res).unwrap();
            for gr in g.val_iter() {
                for v in gr.unsorted_iter() {
                    rres.insert(v.clone());
                }
                //not implemented
                //res.extend(&mut g.ith(i).x.iter());
            }
        }
        res
    }
}
