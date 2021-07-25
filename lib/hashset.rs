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

use ddlog_rt::Closure;
use ddlog_std::{option2std, tuple2, Group, Option as DDOption, Vec as DDVec};

use differential_datalog::record::{CollectionKind, Record};
use im::hashset::{ConsumingIter, HashSet as IMHashSet, OrderedConsumingIter, OrderedIter};
use serde::{
    de::Deserializer,
    ser::{SerializeSeq, Serializer},
};
use std::{
    borrow::Borrow,
    cmp::Ordering,
    collections::hash_map::DefaultHasher,
    fmt::{Debug, Formatter, Result as FmtResult},
    hash::{BuildHasherDefault, Hash, Hasher},
    iter::FromIterator,
    ops::DerefMut,
    result::Result as StdResult,
};

#[derive(Clone)]
pub struct HashSet<T> {
    // We must use a deterministic hasher here instead of the default `RandomState`
    // to ensure that hashset iterators yield elements in a consistent order.
    set: IMHashSet<T, BuildHasherDefault<DefaultHasher>>,
}

impl<T> Default for HashSet<T> {
    fn default() -> Self {
        HashSet {
            set: IMHashSet::with_hasher(<BuildHasherDefault<DefaultHasher>>::default()),
        }
    }
}

impl<T: Hash + Eq + Ord> Hash for HashSet<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Our modified `im::HashSet` implementation ensures that `hash` is
        // deterministic, i.e., given the same hasher and two sets with the
        // same elements it returns the same hash value.  In contrast, the
        // original `im` crate currently violates this property.
        self.set.hash(state);
    }
}

impl<T: Hash + Eq + Ord> PartialEq for HashSet<T> {
    fn eq(&self, other: &Self) -> bool {
        // The `Eq` implementation in `im::HashSet` is unable to rely on the
        // ordered iterator (which requires the `Ord` trait) and is therefore
        // inefficient compared to this.

        // Check if `other` is a clone of `self`.
        if self.set.ptr_eq(&other.set) {
            return true;
        }
        if self.len() != other.len() {
            return false;
        }
        self.iter().eq(other.iter())
    }
}

impl<T: Hash + Eq + Ord> Eq for HashSet<T> {}

impl<T: Hash + Eq + Clone + PartialOrd + Ord> PartialOrd for HashSet<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.set.ptr_eq(&other.set) {
            return Some(Ordering::Equal);
        }
        // We don't need to implement lexicographical ordering.  If the two
        // sets have different sizes, just compare the sizes.
        match self.len().cmp(&other.len()) {
            Ordering::Equal => (),
            ord => return Some(ord),
        }
        // Our modified `im::HashSet` implementation ensures that `partial_cmp`
        // is consistent: given the same hasher and two sets with the
        // same elements, it returns `Ordering::Equal`.  In contrast, the
        // original `im` crate currently violates this property.
        self.set.partial_cmp(&other.set)
    }
}

impl<T: Hash + Eq + Clone + Ord> Ord for HashSet<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.set.ptr_eq(&other.set) {
            return Ordering::Equal;
        }
        match self.len().cmp(&other.len()) {
            Ordering::Equal => (),
            ord => return ord,
        }

        // Our modified `IMHashSet` implementation ensures that `cmp`
        // is consistent: given the same hasher and two sets with the
        // same elements it returns `Ordering::Equal`.  In contrast, the
        // original `im` crate currently violates this property.
        self.set.cmp(&other.set)
    }
}

impl<T: Serialize + Hash + Eq + Clone + Ord> Serialize for HashSet<T> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Don't use IMHashSet's implementation of `serialize` to
        // ensure deterministic order.
        let mut s = serializer.serialize_seq(Some(self.len()))?;
        for i in self.iter() {
            s.serialize_element(i)?;
        }
        s.end()
    }
}

impl<'de, T: Deserialize<'de> + Hash + Eq + Clone> Deserialize<'de> for HashSet<T> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        IMHashSet::deserialize(deserializer).map(HashSet::from)
    }
}

impl<T> HashSet<T> {
    pub fn len(&self) -> usize {
        self.set.len()
    }
}

impl<'a, T: Hash + Ord> HashSet<T> {
    // Always use this iterator over `im::HashSet::iter` in scenarios where
    // order matters (serialization, formatting, etc.).
    pub fn iter(&'a self) -> OrderedIter<'a, T> {
        // The default IMHashSet iterator does not guarantee consistent
        // ordering: two sets with the same elements can iterate over them
        // in different order even when using the same hasher.  This can
        // lead to non-deterministic behavior in DDlog code that iterates
        // over hashsets.  We must therefore use a deterministic iterator.
        self.set.ordered_iter()
    }
}

impl<T: Hash + Eq + Clone + Ord> IntoIterator for HashSet<T> {
    type Item = T;
    type IntoIter = OrderedConsumingIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        // Use ordered iterator that guarantees consistent ordering.
        self.set.into_ordered_iter()
    }
}

impl<T: Hash + Eq + Clone + From<RT>, RT> FromIterator<RT> for HashSet<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = RT>,
    {
        HashSet {
            set: IMHashSet::from_iter(iter),
        }
    }
}

impl<T> HashSet<T> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

impl<T> From<IMHashSet<T, BuildHasherDefault<DefaultHasher>>> for HashSet<T> {
    fn from(set: IMHashSet<T, BuildHasherDefault<DefaultHasher>>) -> Self {
        HashSet { set }
    }
}

impl<T: Hash + Eq> HashSet<T> {
    pub fn contains<BT: ?Sized>(&self, x: &BT) -> bool
    where
        BT: Hash + Eq,
        T: Borrow<BT>,
    {
        self.set.contains(x)
    }
}

impl<T: Hash + Eq + Clone> HashSet<T> {
    pub fn unit(x: T) -> Self {
        let mut set = IMHashSet::default();
        set.insert(x);
        HashSet::from(set)
    }

    pub fn insert(&mut self, x: T) -> Option<T> {
        self.set.insert(x)
    }

    pub fn remove<BT: ?Sized>(&mut self, x: &BT) -> Option<T>
    where
        BT: Hash + Eq,
        T: Borrow<BT>,
    {
        self.set.remove(x)
    }

    pub fn without(&self, x: &T) -> Self {
        HashSet::from(self.set.without(x))
    }

    pub fn update(&self, x: T) -> Self {
        HashSet::from(self.set.update(x))
    }

    pub fn union(self, other: Self) -> Self {
        // `im::HashSet` implements union by adding elements from `other` to
        // `self`.  This is inefficient when `other` is much
        // larger than `self`.  The following optimization will become
        // unnecessary once this PR or something similar has landed:
        // https://github.com/bodil/im-rs/pull/163.
        if self.len() >= other.len() {
            HashSet::from(self.set.union(other.set))
        } else {
            HashSet::from(other.set.union(self.set))
        }
    }
    pub fn unions<I>(i: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        let mut sets: Vec<_> = i.into_iter().collect();

        // Start with the largest set.
        if let Some((largest_idx, _)) = sets.iter().enumerate().max_by_key(|(_, s)| s.len()) {
            let mut largest_set = Self::default();
            std::mem::swap(&mut largest_set, &mut sets[largest_idx]);

            sets.into_iter().fold(largest_set, Self::union)
        } else {
            Self::default()
        }
    }
    pub fn intersection(self, other: Self) -> Self {
        HashSet::from(self.set.intersection(other.set))
    }
    pub fn difference(self, other: Self) -> Self {
        HashSet::from(self.set.difference(other.set))
    }
}

// I don't want to implement `Deref` for `HashSet` to prevent
// accidentally calling `im::HashSet` methods.  For example,
// `im::HashSet::iter` returns the non-deterministic iterator (which can
// mess up DDlog in ways one can easily miss unless they test on big
// enough sets).

// impl<T> Deref for HashSet<T> {
//     type Target = IMHashSet<T>;
//
//     fn deref(//self) -> //Self::Target {
//         //self.set
//     }
// }
//
// impl<T> DerefMut for HashSet<T> {
//     fn deref_mut(//mut self) -> //mut Self::Target {
//         //mut self.set
//     }
// }

impl<T: FromRecord + Hash + Eq + Clone + Ord> FromRecordInner for HashSet<T> {
    fn from_record_inner(val: &Record) -> StdResult<Self, String> {
        match val {
            Record::Array(_, args) => StdResult::from_iter(args.iter().map(T::from_record)),
            v => T::from_record(v).map(Self::unit),
        }
    }
}

impl<T: IntoRecord + Hash + Eq + Clone + Ord> IntoRecord for HashSet<T> {
    fn into_record(self) -> Record {
        Record::Array(
            CollectionKind::Set,
            self.into_iter().map(|x| x.into_record()).collect(),
        )
    }
}

// Set update semantics: update contains values that are in one of the sets
// but not the other.
impl<T: FromRecord + ::serde::de::DeserializeOwned + Hash + Eq + Clone + Ord>
    MutatorInner<HashSet<T>> for Record
{
    fn mutate_inner(&self, set: &mut HashSet<T>) -> StdResult<(), String> {
        let upd = <HashSet<T>>::from_record(self)?;
        *set = HashSet::from(set.set.clone().symmetric_difference(upd.set));
        Ok(())
    }
}

impl<T: Hash + Eq + Debug + Ord> Debug for HashSet<T> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_set().entries(self.iter()).finish()
    }
}

pub fn hashset_size<T>(s: &HashSet<T>) -> std_usize {
    s.len() as std_usize
}

pub fn hashset_empty<T>() -> HashSet<T> {
    HashSet::new()
}

pub fn hashset_singleton<T: Hash + Eq + Clone>(v: &T) -> HashSet<T> {
    HashSet::unit((*v).clone())
}

pub fn hashset_insert<T: Hash + Eq + Clone>(s: &mut HashSet<T>, v: &T) {
    s.insert(v.clone());
}

pub fn hashset_insert_imm<T: Hash + Eq + Clone>(s: &HashSet<T>, v: &T) -> HashSet<T> {
    s.update((*v).clone())
}

pub fn hashset_remove<T: Hash + Eq + Clone>(s: &mut HashSet<T>, v: &T) {
    s.remove(v);
}

pub fn hashset_remove_imm<T: Hash + Eq + Clone>(s: &HashSet<T>, v: &T) -> HashSet<T> {
    s.without(v)
}

pub fn hashset_contains<T: Hash + Eq>(s: &HashSet<T>, v: &T) -> bool {
    s.contains(v)
}

pub fn hashset_is_empty<T>(s: &HashSet<T>) -> bool {
    s.is_empty()
}

pub fn hashset_nth<T: Hash + Clone + Ord>(s: &HashSet<T>, n: &std_usize) -> DDOption<T> {
    option2std(s.iter().nth(*n as usize).cloned())
}

pub fn hashset_to_vec<T: Hash + Clone + Ord>(s: &HashSet<T>) -> DDVec<T> {
    DDVec {
        vec: s.iter().cloned().collect(),
    }
}

pub fn hashset_union<T: Hash + Eq + Clone>(s1: &HashSet<T>, s2: &HashSet<T>) -> HashSet<T> {
    s1.clone().union(s2.clone())
}

pub fn hashset_unions<T: Hash + Eq + Clone + Ord>(sets: &Vec<HashSet<T>>) -> HashSet<T> {
    HashSet::unions(sets.iter().cloned())
}

pub fn group_hashset_unions<K, T: Hash + Eq + Clone + Ord>(
    sets: &Group<K, HashSet<T>>,
) -> HashSet<T> {
    HashSet::unions(sets.iter().map(|tuple2(x, _)| x))
}

pub fn hashset_intersection<T: Hash + Eq + Clone>(s1: &HashSet<T>, s2: &HashSet<T>) -> HashSet<T> {
    s1.clone().intersection(s2.clone())
}

pub fn hashset_difference<T: Hash + Eq + Clone>(s1: &HashSet<T>, s2: &HashSet<T>) -> HashSet<T> {
    s1.clone().difference(s2.clone())
}

pub fn hashset_arg_min<T: Hash + Clone + Ord, B: Ord>(
    s: &HashSet<T>,
    f: &Box<dyn Closure<*const T, B>>,
) -> DDOption<T> {
    DDOption::from(s.iter().min_by_key(|x| f.call(*x)).map(|x| (*x).clone()))
}

pub fn hashset_arg_max<T: Hash + Clone + Ord, B: Ord>(
    s: &HashSet<T>,
    f: &Box<dyn Closure<*const T, B>>,
) -> DDOption<T> {
    DDOption::from(s.iter().max_by_key(|x| f.call(*x)).map(|x| (*x).clone()))
}
