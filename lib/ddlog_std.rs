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

use abomonation::Abomonation;
/// Rust implementation of DDlog standard library functions and types.
use differential_datalog::record::{arg_extract, Record};
use differential_datalog::triomphe::Arc;
use fnv::FnvHasher;
use num::Zero;
use serde::{
    de::{DeserializeOwned, Deserializer},
    ser::{SerializeStruct, Serializer},
};
use std::{
    borrow,
    cmp::{self, Ordering},
    collections::{btree_map, btree_set, BTreeMap, BTreeSet},
    fmt::{self, Debug, Display, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    io,
    iter::FromIterator,
    mem,
    ops::{self, Add, DerefMut},
    option::Option as StdOption,
    result::Result as StdResult,
    slice, str,
    sync::Arc as StdArc,
    vec::{self, Vec as StdVec},
};

const XX_SEED1: u64 = 0x23b691a751d0e108;
const XX_SEED2: u64 = 0x20b09801dce5ff84;

pub fn default<T: Default>() -> T {
    T::default()
}

// Result

/// Convert Rust result type to DDlog's std::Result
pub fn res2std<T, E: Display>(res: StdResult<T, E>) -> Result<T, String> {
    match res {
        Ok(res) => Result::Ok { res },
        Err(e) => Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn result_unwrap_or_default<T: Default + Clone, E>(res: &Result<T, E>) -> T {
    match res {
        Result::Ok { res } => res.clone(),
        Result::Err { err } => T::default(),
    }
}

// Ref

/// An atomically reference counted reference
#[derive(Eq, PartialOrd, PartialEq, Ord, Clone, Hash)]
pub struct Ref<T> {
    x: Arc<T>,
}

impl<T: Default> Default for Ref<T> {
    fn default() -> Self {
        Self {
            x: Arc::new(T::default()),
        }
    }
}

impl<T> Deref for Ref<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*self.x
    }
}

impl<T> From<T> for Ref<T> {
    fn from(x: T) -> Self {
        Self { x: Arc::new(x) }
    }
}

impl<T: Abomonation> Abomonation for Ref<T> {
    unsafe fn entomb<W: io::Write>(&self, write: &mut W) -> io::Result<()> {
        self.deref().entomb(write)
    }

    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> StdOption<&'b mut [u8]> {
        Arc::get_mut(&mut self.x).unwrap().exhume(bytes)
    }

    fn extent(&self) -> usize {
        self.deref().extent()
    }
}

impl<T> Ref<T> {
    pub fn get_mut(this: &mut Self) -> StdOption<&mut T> {
        Arc::get_mut(&mut this.x)
    }
}

impl<T: Display> Display for Ref<T> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        self.deref().fmt(f)
    }
}

impl<T: Debug> Debug for Ref<T> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        self.deref().fmt(f)
    }
}

impl<T: Serialize> Serialize for Ref<T> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.deref().serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Ref<T> {
    fn deserialize<D>(deserializer: D) -> StdResult<Ref<T>, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::from)
    }
}

impl<T: FromRecord> FromRecord for Ref<T> {
    fn from_record(val: &Record) -> StdResult<Self, String> {
        T::from_record(val).map(Self::from)
    }
}

impl<T: IntoRecord + Clone> IntoRecord for Ref<T> {
    fn into_record(self) -> Record {
        (*self.x).clone().into_record()
    }
}

impl<T> Mutator<Ref<T>> for Record
where
    T: Clone,
    Record: Mutator<T>,
{
    fn mutate(&self, arc: &mut Ref<T>) -> StdResult<(), String> {
        let mut copy: T = (*arc).deref().clone();
        self.mutate(&mut copy)?;
        *arc = Ref::from(copy);

        Ok(())
    }
}

pub fn ref_new<A: Clone>(x: &A) -> Ref<A> {
    Ref::from(x.clone())
}

pub fn deref<A: Clone>(x: &Ref<A>) -> &A {
    x.deref()
}

// Arithmetic functions
pub fn u8_pow32(base: &u8, exp: &u32) -> u8 {
    base.wrapping_pow(*exp)
}
pub fn u16_pow32(base: &u16, exp: &u32) -> u16 {
    base.wrapping_pow(*exp)
}
pub fn u32_pow32(base: &u32, exp: &u32) -> u32 {
    base.wrapping_pow(*exp)
}
pub fn u64_pow32(base: &u64, exp: &u32) -> u64 {
    base.wrapping_pow(*exp)
}
pub fn u128_pow32(base: &u128, exp: &u32) -> u128 {
    base.wrapping_pow(*exp)
}
pub fn s8_pow32(base: &i8, exp: &u32) -> i8 {
    base.wrapping_pow(*exp)
}
pub fn s16_pow32(base: &i16, exp: &u32) -> i16 {
    base.wrapping_pow(*exp)
}
pub fn s32_pow32(base: &i32, exp: &u32) -> i32 {
    base.wrapping_pow(*exp)
}
pub fn s64_pow32(base: &i64, exp: &u32) -> i64 {
    base.wrapping_pow(*exp)
}
pub fn s128_pow32(base: &i128, exp: &u32) -> i128 {
    base.wrapping_pow(*exp)
}
pub fn bigint_pow32(base: &ddlog_bigint::Int, exp: &u32) -> ddlog_bigint::Int {
    num::pow::pow(base.clone(), *exp as usize)
}

// Option
impl<T: Copy> Copy for Option<T> {}

pub fn option2std<T>(x: StdOption<T>) -> Option<T> {
    match x {
        StdOption::None => Option::None,
        StdOption::Some(v) => Option::Some { x: v },
    }
}

pub fn std2option<T>(x: Option<T>) -> StdOption<T> {
    match x {
        Option::None => StdOption::None,
        Option::Some { x } => StdOption::Some(x),
    }
}

impl<T> From<StdOption<T>> for Option<T> {
    fn from(x: StdOption<T>) -> Self {
        option2std(x)
    }
}

// this requires Rust 1.41+
impl<T> From<Option<T>> for StdOption<T> {
    fn from(x: Option<T>) -> Self {
        std2option(x)
    }
}

impl<T> FromRecord for Option<T>
where
    T: FromRecord + DeserializeOwned + Default,
{
    fn from_record(val: &Record) -> StdResult<Self, String> {
        match val {
            Record::PosStruct(constr, args) => match constr.as_ref() {
                "ddlog_std::None" if args.len() == 0 => Ok(Option::None {}),
                "ddlog_std::Some" if args.len() == 1 => Ok(Option::Some {
                    x: <T>::from_record(&args[0])?,
                }),
                c => StdResult::Err(format!(
                    "unknown constructor {} of type Option in {:?}",
                    c, *val
                )),
            },

            Record::NamedStruct(constr, args) => match constr.as_ref() {
                "ddlog_std::None" => Ok(Option::None {}),
                "ddlog_std::Some" => Ok(Option::Some {
                    x: arg_extract::<T>(args, "x")?,
                }),
                c => StdResult::Err(format!(
                    "unknown constructor {} of type Option in {:?}",
                    c, *val
                )),
            },

            // `Option` encoded as an array of size 0 or 1.  This is, for instance, useful when
            // interfacing with OVSDB.
            Record::Array(kind, records) => match (records.len()) {
                0 => Ok(Option::None {}),
                1 => Ok(Option::Some {
                    x: T::from_record(&records[0])?,
                }),
                n => Err(format!(
                    "cannot deserialize ddlog_std::Option from container of size {:?}",
                    n
                )),
            },

            Record::Serialized(format, s) => {
                if format == "json" {
                    serde_json::from_str(&*s).map_err(|e| format!("{}", e))
                } else {
                    StdResult::Err(format!("unsupported serialization format '{}'", format))
                }
            }

            v => {
                // Finally, assume that the record contains the inner value of a `Some`.
                // XXX: this introduces ambiguity, as an array could represent either the inner
                // value or an array encoding of `Option`.
                Ok(Option::Some {
                    x: T::from_record(&v)?,
                })
            }
        }
    }
}

pub fn option_unwrap_or_default<T: Default + Clone>(opt: &Option<T>) -> T {
    match opt {
        Option::Some { x } => x.clone(),
        Option::None => T::default(),
    }
}

// Range
pub fn range_vec<A: Clone + Ord + Add<Output = A> + PartialOrd + Zero>(
    from: &A,
    to: &A,
    step: &A,
) -> Vec<A> {
    let mut vec = Vec::new();
    let mut x = from.clone();

    if step < &A::zero() {
        while x > *to {
            vec.push(x.clone());
            x = x + step.clone();
        }
    } else if step > &A::zero() {
        while x < *to {
            vec.push(x.clone());
            x = x + step.clone();
        }
    }

    vec
}

/// A contiguous growable array type mirroring [`Vec`]
///
/// [`Vec`]: https://doc.rust-lang.org/std/vec/struct.Vec.html
#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Default)]
pub struct Vec<T> {
    pub vec: StdVec<T>,
}

impl<T> Vec<T> {
    /// Creates a new, empty vector
    pub const fn new() -> Self {
        Vec { vec: StdVec::new() }
    }

    /// Creates a new, empty `Vec<T>` with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Vec {
            vec: StdVec::with_capacity(capacity),
        }
    }

    /// Returns an iterator over the vector
    pub fn iter<'a>(&'a self) -> VecIter<'a, T> {
        VecIter::new(self)
    }
}

impl<T: Clone> Vec<T> {
    pub fn resize(&mut self, new_len: usize, value: &T) {
        self.vec.resize_with(new_len, || value.clone());
    }
}

impl<T: Clone> From<&[T]> for Vec<T> {
    fn from(slice: &[T]) -> Self {
        Vec {
            vec: slice.to_vec(),
        }
    }
}

impl<T> FromIterator<T> for Vec<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self {
            vec: ::std::vec::Vec::from_iter(iter),
        }
    }
}

impl<T> From<StdVec<T>> for Vec<T> {
    fn from(vec: StdVec<T>) -> Self {
        Vec { vec }
    }
}

impl<T> Deref for Vec<T> {
    type Target = StdVec<T>;

    fn deref(&self) -> &Self::Target {
        &self.vec
    }
}

impl<T> DerefMut for Vec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.vec
    }
}

impl<T> AsRef<StdVec<T>> for Vec<T> {
    fn as_ref(&self) -> &StdVec<T> {
        &self.vec
    }
}

impl<T> AsMut<StdVec<T>> for Vec<T> {
    fn as_mut(&mut self) -> &mut StdVec<T> {
        &mut self.vec
    }
}

impl<T> AsRef<[T]> for Vec<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> AsMut<[T]> for Vec<T> {
    fn as_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T: Serialize> Serialize for Vec<T> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.vec.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Vec<T> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        StdVec::deserialize(deserializer).map(|vec| Vec { vec })
    }
}

impl<T: FromRecord> FromRecord for Vec<T> {
    fn from_record(val: &Record) -> StdResult<Self, String> {
        StdVec::from_record(val).map(|vec| Vec { vec })
    }
}

impl<T: IntoRecord> IntoRecord for Vec<T> {
    fn into_record(self) -> Record {
        self.vec.into_record()
    }
}

impl<T: FromRecord> Mutator<Vec<T>> for Record {
    fn mutate(&self, vec: &mut Vec<T>) -> StdResult<(), String> {
        self.mutate(&mut vec.vec)
    }
}

impl<T: Debug> Debug for Vec<T> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_list().entries(self.vec.iter()).finish()
    }
}

impl<T> IntoIterator for Vec<T> {
    type Item = T;
    type IntoIter = vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.vec.into_iter()
    }
}

// This is needed so we can support for-loops over `Vec`'s
pub struct VecIter<'a, X> {
    iter: slice::Iter<'a, X>,
}

impl<'a, X> VecIter<'a, X> {
    pub fn new(vec: &'a Vec<X>) -> VecIter<'a, X> {
        VecIter {
            iter: vec.vec.iter(),
        }
    }
}

impl<'a, X> Iterator for VecIter<'a, X> {
    type Item = &'a X;

    fn next(&mut self) -> StdOption<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, StdOption<usize>) {
        self.iter.size_hint()
    }
}

/// Get the length of a vec
pub fn vec_len<T>(vec: &Vec<T>) -> std_usize {
    vec.len() as std_usize
}

/// Create a new, empty vec
pub const fn vec_empty<T>() -> Vec<T> {
    Vec::new()
}

/// Create an fill a vec with `len` copies of `splat`
pub fn vec_with_length<T: Clone>(len: &std_usize, splat: &T) -> Vec<T> {
    let mut res = Vec::with_capacity(*len as usize);
    res.resize(*len as usize, splat);
    res
}

/// Create a new vec with the requested capacity
pub fn vec_with_capacity<T>(len: &std_usize) -> Vec<T> {
    Vec::with_capacity(*len as usize)
}

/// Create a new vec with a single element
pub fn vec_singleton<T: Clone>(value: &T) -> Vec<T> {
    Vec {
        vec: vec![value.clone()],
    }
}

pub fn vec_append<T: Clone>(vec: &mut Vec<T>, other: &Vec<T>) {
    vec.extend_from_slice(other.as_slice());
}

pub fn vec_push<T: Clone>(vec: &mut Vec<T>, elem: &T) {
    vec.push(elem.clone());
}

/// Pushes to a vector immutably by creating an entirely new vector and pushing the new
/// element to it
pub fn vec_push_imm<T: Clone>(immutable_vec: &Vec<T>, x: &T) -> Vec<T> {
    let mut mutable_vec = Vec::with_capacity(immutable_vec.len());
    mutable_vec.extend_from_slice(immutable_vec.as_slice());
    mutable_vec.push(x.clone());

    mutable_vec
}

pub fn vec_pop<X: Ord + Clone>(v: &mut Vec<X>) -> Option<X> {
    option2std(v.pop())
}

pub fn vec_contains<T: PartialEq>(vec: &Vec<T>, x: &T) -> bool {
    vec.contains(x)
}

pub fn vec_is_empty<T>(vec: &Vec<T>) -> bool {
    vec.is_empty()
}

pub fn vec_nth<T: Clone>(vec: &Vec<T>, nth: &std_usize) -> Option<T> {
    vec.get(*nth as usize).cloned().into()
}

pub fn vec_to_set<T: Ord + Clone>(vec: &Vec<T>) -> Set<T> {
    Set {
        x: vec.vec.iter().cloned().collect(),
    }
}

pub fn vec_sort<T: Ord>(vec: &mut Vec<T>) {
    vec.as_mut_slice().sort();
}

pub fn vec_sort_imm<T: Ord + Clone>(vec: &Vec<T>) -> Vec<T> {
    let mut res = (*vec).clone();
    res.vec.sort();
    res
}

pub fn vec_resize<T: Clone>(vec: &mut Vec<T>, new_len: &std_usize, value: &T) {
    vec.resize(*new_len as usize, value)
}

pub fn vec_truncate<T>(vec: &mut Vec<T>, new_len: &std_usize) {
    vec.vec.truncate(*new_len as usize)
}

pub fn vec_swap_nth<T: Clone>(vec: &mut Vec<T>, idx: &std_usize, value: &mut T) -> bool {
    if (*idx as usize) < vec.len() {
        mem::swap(&mut vec[*idx as usize], value);
        true
    } else {
        false
    }
}

pub fn vec_update_nth<T: Clone>(vec: &mut Vec<T>, idx: &std_usize, value: &T) -> bool {
    if (*idx as usize) < vec.len() {
        vec[*idx as usize] = value.clone();
        true
    } else {
        false
    }
}

pub fn vec_zip<X: Clone, Y: Clone>(v1: &Vec<X>, v2: &Vec<Y>) -> Vec<tuple2<X, Y>> {
    Vec {
        vec: v1
            .iter()
            .zip(v2.iter())
            .map(|(x, y)| tuple2(x.clone(), y.clone()))
            .collect(),
    }
}

// Set

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Default)]
pub struct Set<T: Ord> {
    pub x: BTreeSet<T>,
}

impl<T: Ord + Serialize> Serialize for Set<T> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.x.serialize(serializer)
    }
}

impl<'de, T: Ord + Deserialize<'de>> Deserialize<'de> for Set<T> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeSet::deserialize(deserializer).map(|x| Set { x })
    }
}

/* This is needed so we can support for-loops over `Set`'s
 */
pub struct SetIter<'a, X> {
    iter: btree_set::Iter<'a, X>,
}

impl<'a, T> SetIter<'a, T> {
    pub fn new(set: &'a Set<T>) -> SetIter<'a, T>
    where
        T: Ord,
    {
        SetIter { iter: set.x.iter() }
    }
}

impl<'a, X> Iterator for SetIter<'a, X> {
    type Item = &'a X;

    fn next(&mut self) -> StdOption<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, StdOption<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, T: Ord> Set<T> {
    pub fn iter(&'a self) -> SetIter<'a, T> {
        SetIter::new(self)
    }
}

impl<T: Ord> Set<T> {
    pub fn new() -> Self {
        Set { x: BTreeSet::new() }
    }

    pub fn insert(&mut self, v: T) {
        self.x.insert(v);
    }
}

impl<T: FromRecord + Ord> FromRecord for Set<T> {
    fn from_record(val: &Record) -> StdResult<Self, String> {
        BTreeSet::from_record(val).map(|x| Set { x })
    }
}

impl<T: IntoRecord + Ord> IntoRecord for Set<T> {
    fn into_record(self) -> Record {
        self.x.into_record()
    }
}

impl<T: FromRecord + Ord> Mutator<Set<T>> for Record {
    fn mutate(&self, set: &mut Set<T>) -> StdResult<(), String> {
        self.mutate(&mut set.x)
    }
}

impl<T: Ord> IntoIterator for Set<T> {
    type Item = T;
    type IntoIter = btree_set::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.x.into_iter()
    }
}

impl<T: Ord> FromIterator<T> for Set<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Set {
            x: BTreeSet::from_iter(iter),
        }
    }
}

impl<T: Debug + Ord> Debug for Set<T> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_set().entries(self.x.iter()).finish()
    }
}

pub fn set_size<X: Ord + Clone>(s: &Set<X>) -> std_usize {
    s.x.len() as std_usize
}

pub fn set_empty<X: Clone + Ord>() -> Set<X> {
    Set::new()
}

pub fn set_singleton<X: Ord + Clone>(v: &X) -> Set<X> {
    let mut s = Set::new();
    s.insert(v.clone());
    s
}

pub fn set_insert<X: Ord + Clone>(s: &mut Set<X>, v: &X) {
    s.x.insert((*v).clone());
}

pub fn set_insert_imm<X: Ord + Clone>(s: &Set<X>, v: &X) -> Set<X> {
    let mut s2 = s.clone();
    s2.insert((*v).clone());
    s2
}

pub fn set_contains<X: Ord>(s: &Set<X>, v: &X) -> bool {
    s.x.contains(v)
}

pub fn set_is_empty<X: Ord>(s: &Set<X>) -> bool {
    s.x.is_empty()
}

pub fn set_nth<X: Ord + Clone>(s: &Set<X>, n: &std_usize) -> Option<X> {
    option2std(s.x.iter().nth(*n as usize).cloned())
}

pub fn set_to_vec<X: Ord + Clone>(set: &Set<X>) -> Vec<X> {
    Vec {
        vec: set.x.iter().cloned().collect(),
    }
}

pub fn set_union<X: Ord + Clone>(s1: &Set<X>, s2: &Set<X>) -> Set<X> {
    let mut s = s1.clone();
    s.x.append(&mut s2.x.clone());
    s
}

pub fn set_unions<X: Ord + Clone>(sets: &Vec<Set<X>>) -> Set<X> {
    let mut s = BTreeSet::new();
    for set in sets.iter() {
        s.append(&mut set.x.clone());
    }

    Set { x: s }
}

pub fn set_intersection<X: Ord + Clone>(s1: &Set<X>, s2: &Set<X>) -> Set<X> {
    Set {
        x: s1.x.intersection(&s2.x).cloned().collect(),
    }
}

pub fn set_difference<X: Ord + Clone>(s1: &Set<X>, s2: &Set<X>) -> Set<X> {
    Set {
        x: s1.x.difference(&s2.x).cloned().collect(),
    }
}

// Map

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Default)]
pub struct Map<K: Ord, V> {
    pub x: BTreeMap<K, V>,
}

impl<K: Ord + Serialize, V: Serialize> Serialize for Map<K, V> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.x.serialize(serializer)
    }
}

impl<'de, K: Ord + Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for Map<K, V> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeMap::deserialize(deserializer).map(|x| Map { x })
    }
}

// This is needed so we can support for-loops over `Map`'s
pub struct MapIter<'a, K, V> {
    iter: btree_map::Iter<'a, K, V>,
}

impl<'a, K: Ord, V> MapIter<'a, K, V> {
    pub fn new(map: &'a Map<K, V>) -> MapIter<'a, K, V> {
        MapIter { iter: map.x.iter() }
    }
}

impl<'a, K: Clone, V: Clone> Iterator for MapIter<'a, K, V> {
    type Item = tuple2<K, V>;

    fn next(&mut self) -> StdOption<Self::Item> {
        self.iter.next().map(|(k, v)| tuple2(k.clone(), v.clone()))
    }

    fn size_hint(&self) -> (usize, StdOption<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, K: Ord, V> Map<K, V> {
    pub fn iter(&'a self) -> MapIter<'a, K, V> {
        MapIter::new(self)
    }
}

impl<K: Ord, V> Map<K, V> {
    pub fn new() -> Self {
        Map { x: BTreeMap::new() }
    }

    pub fn insert(&mut self, k: K, v: V) {
        self.x.insert(k, v);
    }
}

impl<K: FromRecord + Ord, V: FromRecord> FromRecord for Map<K, V> {
    fn from_record(val: &Record) -> StdResult<Self, String> {
        BTreeMap::from_record(val).map(|x| Map { x })
    }
}

impl<K: IntoRecord + Ord, V: IntoRecord> IntoRecord for Map<K, V> {
    fn into_record(self) -> Record {
        self.x.into_record()
    }
}

impl<K: FromRecord + Ord, V: FromRecord + PartialEq> Mutator<Map<K, V>> for Record {
    fn mutate(&self, map: &mut Map<K, V>) -> StdResult<(), String> {
        self.mutate(&mut map.x)
    }
}

pub struct MapIntoIter<K, V> {
    iter: btree_map::IntoIter<K, V>,
}

impl<K: Ord, V> MapIntoIter<K, V> {
    pub fn new(map: Map<K, V>) -> MapIntoIter<K, V> {
        MapIntoIter {
            iter: map.x.into_iter(),
        }
    }
}

impl<K, V> Iterator for MapIntoIter<K, V> {
    type Item = tuple2<K, V>;

    fn next(&mut self) -> StdOption<Self::Item> {
        self.iter.next().map(|(k, v)| tuple2(k, v))
    }

    fn size_hint(&self) -> (usize, StdOption<usize>) {
        self.iter.size_hint()
    }
}

impl<K: Ord, V> IntoIterator for Map<K, V> {
    type Item = tuple2<K, V>;
    type IntoIter = MapIntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter::new(self)
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for Map<K, V> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        Map {
            x: BTreeMap::from_iter(iter),
        }
    }
}

impl<K: Debug + Ord, V: Debug> Debug for Map<K, V> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_map().entries(self.x.iter()).finish()
    }
}

pub fn map_size<K: Ord, V>(m: &Map<K, V>) -> std_usize {
    m.x.len() as std_usize
}

pub fn map_empty<K: Ord + Clone, V: Clone>() -> Map<K, V> {
    Map::new()
}

pub fn map_singleton<K: Ord + Clone, V: Clone>(k: &K, v: &V) -> Map<K, V> {
    let mut m = Map::new();
    m.insert(k.clone(), v.clone());
    m
}

pub fn map_insert<K: Ord + Clone, V: Clone>(m: &mut Map<K, V>, k: &K, v: &V) {
    m.x.insert((*k).clone(), (*v).clone());
}

pub fn map_remove<K: Ord + Clone, V: Clone>(m: &mut Map<K, V>, k: &K) -> Option<V> {
    option2std(m.x.remove(k))
}

pub fn map_insert_imm<K: Ord + Clone, V: Clone>(m: &Map<K, V>, k: &K, v: &V) -> Map<K, V> {
    let mut m2 = m.clone();
    m2.insert((*k).clone(), (*v).clone());
    m2
}

pub fn map_get<K: Ord, V: Clone>(m: &Map<K, V>, k: &K) -> Option<V> {
    option2std(m.x.get(k).cloned())
}

pub fn map_contains_key<K: Ord, V: Clone>(s: &Map<K, V>, k: &K) -> bool {
    s.x.contains_key(k)
}

pub fn map_is_empty<K: Ord, V: Clone>(m: &Map<K, V>) -> bool {
    m.x.is_empty()
}

pub fn map_union<K: Ord + Clone, V: Clone>(m1: &Map<K, V>, m2: &Map<K, V>) -> Map<K, V> {
    let mut m = m1.clone();
    m.x.append(&mut m2.x.clone());
    m
}

pub fn map_keys<K: Ord + Clone, V>(map: &Map<K, V>) -> Vec<K> {
    Vec {
        vec: map.x.keys().cloned().collect(),
    }
}

// strings

pub fn __builtin_2string<T: Display>(x: &T) -> String {
    format!("{}", *x)
}

pub fn hex<T: fmt::LowerHex>(x: &T) -> String {
    format!("{:x}", *x)
}

pub fn parse_dec_u64(s: &String) -> Option<u64> {
    option2std(s.parse::<u64>().ok())
}

pub fn parse_dec_i64(s: &String) -> Option<i64> {
    option2std(s.parse::<i64>().ok())
}

pub fn string_join(strings: &Vec<String>, sep: &String) -> String {
    strings.join(sep.as_str())
}

pub fn string_split(string: &String, sep: &String) -> Vec<String> {
    Vec {
        vec: string.split(sep).map(|x| x.to_owned()).collect(),
    }
}

pub fn string_contains(s1: &String, s2: &String) -> bool {
    s1.contains(s2.as_str())
}

pub fn from_utf8(v: &[u8]) -> Result<String, String> {
    res2std(str::from_utf8(v).map(|s| s.to_string()))
}

pub fn encode_utf16(s: &String) -> Vec<u16> {
    s.encode_utf16().collect()
}

pub fn from_utf16(v: &[u16]) -> Result<String, String> {
    res2std(String::from_utf16(v).map(|s| s.to_string()))
}

pub fn string_substr(s: &String, start: &std_usize, end: &std_usize) -> String {
    let len = s.len();
    let from = cmp::min(*start as usize, len);
    let to = cmp::max(from, cmp::min(*end as usize, len));
    s[from..to].to_string()
}

pub fn string_replace(s: &String, from: &String, to: &String) -> String {
    s.replace(from, to)
}

pub fn string_starts_with(s: &String, prefix: &String) -> bool {
    s.starts_with(prefix)
}

pub fn string_ends_with(s: &String, suffix: &String) -> bool {
    s.ends_with(suffix)
}

pub fn string_trim(s: &String) -> String {
    s.trim().to_string()
}

pub fn string_len(s: &String) -> std_usize {
    s.len() as std_usize
}

pub fn string_to_bytes(s: &String) -> Vec<u8> {
    Vec::from(s.as_bytes())
}

pub fn str_to_lower(s: &String) -> String {
    s.to_lowercase()
}

pub fn string_to_lowercase(s: &String) -> String {
    s.to_lowercase()
}

pub fn string_to_uppercase(s: &String) -> String {
    s.to_uppercase()
}

pub fn string_reverse(s: &String) -> String {
    s.chars().rev().collect()
}

// Hashing

pub fn hash64<T: Hash>(x: &T) -> u64 {
    let mut hasher = FnvHasher::with_key(XX_SEED1);
    x.hash(&mut hasher);
    hasher.finish()
}

pub fn hash128<T: Hash>(x: &T) -> u128 {
    let mut hasher = FnvHasher::with_key(XX_SEED1);
    x.hash(&mut hasher);
    let w1 = hasher.finish();

    let mut hasher = FnvHasher::with_key(XX_SEED2);
    x.hash(&mut hasher);
    let w2 = hasher.finish();

    ((w1 as u128) << 64) | (w2 as u128)
}

pub type ProjectFunc<X> = StdArc<dyn Fn(&DDValue) -> X + Send + Sync>;

/*
 * Group type (returned by the `group_by` operator).
 *
 * A group captures output of the differential dataflow `reduce` operator.
 * Thus, upon creation it is backed by references to DD state.  However, we
 * would like to be able to manipulate groups as normal variables, store then
 * in relations, which requires copying the contents of a group during cloning.
 * Since we want the same code (e.g., the same aggregation functions) to work
 * on both reference-backed and value-backed groups, we represent groups as
 * an enum and provide uniform API over both variants.
 *
 * There is a problem of managing the lifetime of a group.  Since one of the
 * two variants contains references, the group type is parameterized by the
 * lifetime of these refs.  However, in order to be able to freely store and
 * pass groups to and from functions, we want `'static` lifetime.  Because
 * of the way we use groups in DDlog-generated code, we can safely transmute
 * them to the `'static` lifetime upon creation.  Here is why.  A group is
 * always created like this:
 * ```
 * let ref g = GroupEnum::ByRef{key, vals, project}
 * ```
 * where `vals` haa local lifetime `'a` that contains the lifetime
 * `'b` of the resulting reference `g`.  Since we are never going to move
 * `vals` refs out of the group (the accessor API returns them
 * by-value), it is ok to tranmute `g` from `&'b Group<'a>` to
 * `&'b Group<'static>` and have the `'b` lifetime protect access to the group.
 * The only way to use the group outside of `'b` is to clone it, which will
 * create an instance of `ByVal` that truly has `'static` lifetime.
 */

pub type Group<K, V> = GroupEnum<'static, K, V>;

fn test() {
    fn is_sync<T: Send + Sync>() {}
    is_sync::<Group<u8, u8>>(); // compiles only if true
}

pub enum GroupEnum<'a, K, V> {
    ByRef {
        key: K,
        group: &'a [(&'a DDValue, Weight)],
        project: ProjectFunc<V>,
    },
    ByVal {
        key: K,
        group: StdVec<tuple2<V, DDWeight>>,
    },
}

/* Always clone by value. */
impl<K: Clone, V: Clone> Clone for Group<K, V> {
    fn clone(&self) -> Self {
        match self {
            GroupEnum::ByRef {
                key,
                group,
                project,
            } => GroupEnum::ByVal {
                key: key.clone(),
                group: group
                    .iter()
                    .map(|(v, w)| tuple2(project(v), *w as DDWeight))
                    .collect(),
            },
            GroupEnum::ByVal { key, group } => GroupEnum::ByVal {
                key: key.clone(),
                group: group.clone(),
            },
        }
    }
}

impl<K: Default, V: Default> Default for Group<K, V> {
    fn default() -> Self {
        GroupEnum::ByVal {
            key: K::default(),
            group: vec![],
        }
    }
}

/* We compare two groups by comparing values returned by their `project()`
 * functions, not the underlying DDValue's.  DDValue's are not visible to
 * the DDlog program; hence two groups are equal iff they have the same
 * projections. */

impl<K: PartialEq, V: Clone + PartialEq> PartialEq for Group<K, V> {
    fn eq(&self, other: &Self) -> bool {
        (self.key_ref() == other.key_ref()) && (self.iter().eq(other.iter()))
    }
}

impl<K: PartialEq, V: Clone + PartialEq> Eq for Group<K, V> {}

impl<K: PartialOrd, V: Clone + PartialOrd> PartialOrd for Group<K, V> {
    fn partial_cmp(&self, other: &Self) -> StdOption<Ordering> {
        match self.key_ref().partial_cmp(other.key_ref()) {
            None => None,
            Some(Ordering::Equal) => self.iter().partial_cmp(other.iter()),
            ord => ord,
        }
    }
}

impl<K: Ord, V: Clone + Ord> Ord for Group<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key_ref().cmp(other.key_ref()) {
            Ordering::Equal => self.iter().cmp(other.iter()),
            ord => ord,
        }
    }
}

/* Likewise, we hash projections, not the underlying DDValue's. */
impl<K: Hash, V: Clone + Hash> Hash for Group<K, V> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.key_ref().hash(state);
        for v in self.iter() {
            v.hash(state);
        }
    }
}

/* We rely on DDlogGroup to serialize/deserialize and print groups. */

impl<K: Clone, V: Clone> DDlogGroup<K, V> {
    pub fn from_group(g: &Group<K, V>) -> Self {
        let vals: StdVec<tuple2<V, DDWeight>> = g.iter().collect();
        DDlogGroup {
            key: g.key(),
            vals: Vec::from(vals),
        }
    }
}

impl<K, V> From<DDlogGroup<K, V>> for Group<K, V> {
    fn from(g: DDlogGroup<K, V>) -> Self {
        Group::new(g.key, g.vals.vec)
    }
}

impl<K: Debug + Clone, V: Debug + Clone> Debug for Group<K, V> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Debug::fmt(&DDlogGroup::from_group(self), f)
    }
}

impl<K: IntoRecord + Clone, V: IntoRecord + Clone> IntoRecord for Group<K, V> {
    fn into_record(self) -> Record {
        DDlogGroup::from_group(&self).into_record()
    }
}

impl<K, V> Mutator<Group<K, V>> for Record
where
    Record: Mutator<K>,
    Record: Mutator<V>,
    K: IntoRecord + FromRecord + Clone + Default + DeserializeOwned,
    V: IntoRecord + FromRecord + Clone + Default + DeserializeOwned,
{
    fn mutate(&self, grp: &mut Group<K, V>) -> StdResult<(), String> {
        let mut dgrp = DDlogGroup::from_group(grp);
        self.mutate(&mut dgrp)?;
        *grp = Group::from(dgrp);
        Ok(())
    }
}

impl<K, V> FromRecord for Group<K, V>
where
    K: Default + FromRecord + serde::de::DeserializeOwned,
    V: Default + FromRecord + serde::de::DeserializeOwned,
{
    fn from_record(rec: &Record) -> StdResult<Self, String> {
        DDlogGroup::from_record(rec).map(|g| Group::from(g))
    }
}

impl<K: Clone + Serialize, V: Clone + Serialize> Serialize for Group<K, V> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        DDlogGroup::from_group(self).serialize(serializer)
    }
}

impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for Group<K, V> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        DDlogGroup::deserialize(deserializer).map(|g| Group::from(g))
    }
}

/* The iterator used to implement for-loops over `Group`'s. */
pub enum GroupIter<'a, V> {
    ByRef {
        iter: slice::Iter<'a, (&'static DDValue, Weight)>,
        project: ProjectFunc<V>,
    },
    ByVal {
        iter: slice::Iter<'a, tuple2<V, DDWeight>>,
    },
}

impl<'a, V> GroupIter<'a, V> {
    pub fn new<K>(grp: &'a Group<K, V>) -> GroupIter<'a, V> {
        match grp {
            GroupEnum::ByRef { group, project, .. } => GroupIter::ByRef {
                iter: group.iter(),
                project: project.clone(),
            },
            GroupEnum::ByVal { group, .. } => GroupIter::ByVal { iter: group.iter() },
        }
    }
}

impl<'a, V: Clone> Iterator for GroupIter<'a, V> {
    type Item = tuple2<V, DDWeight>;

    fn next(&mut self) -> StdOption<Self::Item> {
        match self {
            GroupIter::ByRef { iter, project } => match iter.next() {
                None => None,
                Some((x, w)) => Some(tuple2(project(x), *w as DDWeight)),
            },
            GroupIter::ByVal { iter } => match iter.next() {
                None => None,
                Some(x) => Some(x.clone()),
            },
        }
    }

    fn size_hint(&self) -> (usize, StdOption<usize>) {
        match self {
            GroupIter::ByRef { iter, .. } => iter.size_hint(),
            GroupIter::ByVal { iter } => iter.size_hint(),
        }
    }
}

/* Iterator over group values ignoring element weights. */
pub enum GroupValIter<'a, V> {
    ByRef {
        iter: slice::Iter<'a, (&'static DDValue, Weight)>,
        project: ProjectFunc<V>,
    },
    ByVal {
        iter: slice::Iter<'a, tuple2<V, DDWeight>>,
    },
}

impl<'a, V> GroupValIter<'a, V> {
    pub fn new<K>(grp: &'a Group<K, V>) -> GroupValIter<'a, V> {
        match grp {
            GroupEnum::ByRef { group, project, .. } => GroupValIter::ByRef {
                iter: group.iter(),
                project: project.clone(),
            },
            GroupEnum::ByVal { group, .. } => GroupValIter::ByVal { iter: group.iter() },
        }
    }
}

impl<'a, V: Clone> Iterator for GroupValIter<'a, V> {
    type Item = V;

    fn next(&mut self) -> StdOption<Self::Item> {
        match self {
            GroupValIter::ByRef { iter, project } => match iter.next() {
                None => None,
                Some((x, _)) => Some(project(x)),
            },
            GroupValIter::ByVal { iter } => match iter.next() {
                None => None,
                Some(x) => Some(x.0.clone()),
            },
        }
    }

    fn size_hint(&self) -> (usize, StdOption<usize>) {
        match self {
            GroupValIter::ByRef { iter, .. } => iter.size_hint(),
            GroupValIter::ByVal { iter } => iter.size_hint(),
        }
    }
}

/* The iterator used to implement FlatMap over `Group`'s. */
pub enum GroupIntoIter<V> {
    ByRef {
        iter: slice::Iter<'static, (&'static DDValue, Weight)>,
        project: ProjectFunc<V>,
    },
    ByVal {
        iter: vec::IntoIter<tuple2<V, DDWeight>>,
    },
}

impl<V: Clone> GroupIntoIter<V> {
    pub fn new<K>(grp: Group<K, V>) -> GroupIntoIter<V> {
        match grp {
            GroupEnum::ByRef { group, project, .. } => GroupIntoIter::ByRef {
                iter: group.iter(),
                project: project.clone(),
            },
            GroupEnum::ByVal { group, .. } => GroupIntoIter::ByVal {
                iter: group.into_iter(),
            },
        }
    }
}

impl<V: Clone> Iterator for GroupIntoIter<V> {
    type Item = tuple2<V, DDWeight>;

    fn next(&mut self) -> StdOption<Self::Item> {
        match self {
            GroupIntoIter::ByRef { iter, project } => match iter.next() {
                None => None,
                Some((x, w)) => Some(tuple2(project(x), *w as DDWeight)),
            },
            GroupIntoIter::ByVal { iter } => match iter.next() {
                None => None,
                Some(x) => Some(x),
            },
        }
    }

    fn size_hint(&self) -> (usize, StdOption<usize>) {
        match self {
            GroupIntoIter::ByRef { iter, .. } => iter.size_hint(),
            GroupIntoIter::ByVal { iter } => iter.size_hint(),
        }
    }
}

impl<K, V> Group<K, V> {
    /* Unsafe constructor for use in auto-generated code only. */
    pub unsafe fn new_by_ref<'a>(
        key: K,
        group: &'a [(&'a DDValue, Weight)],
        project: ProjectFunc<V>,
    ) -> Group<K, V> {
        GroupEnum::ByRef {
            key,
            group: ::std::mem::transmute::<_, &'static [(&'static DDValue, Weight)]>(group),
            project,
        }
    }

    pub fn new<'a>(key: K, group: StdVec<tuple2<V, DDWeight>>) -> Group<K, V> {
        GroupEnum::ByVal { key, group }
    }

    pub fn key_ref(&self) -> &K {
        match self {
            GroupEnum::ByRef { key, .. } => key,
            GroupEnum::ByVal { key, .. } => key,
        }
    }

    fn size(&self) -> std_usize {
        match self {
            GroupEnum::ByRef { group, .. } => group.len() as std_usize,
            GroupEnum::ByVal { group, .. } => group.len() as std_usize,
        }
    }
}

impl<K: Clone, V> Group<K, V> {
    /* Extract key by value; use `key_ref` to get a reference to key. */
    pub fn key(&self) -> K {
        match self {
            GroupEnum::ByRef { key, .. } => key.clone(),
            GroupEnum::ByVal { key, .. } => key.clone(),
        }
    }
}

impl<K, V: Clone> Group<K, V> {
    fn first(&self) -> V {
        match self {
            GroupEnum::ByRef { group, project, .. } => project(group[0].0),
            GroupEnum::ByVal { group, .. } => group[0].0.clone(),
        }
    }

    fn nth_unchecked(&self, n: std_usize) -> V {
        match self {
            GroupEnum::ByRef { group, project, .. } => project(group[n as usize].0),
            GroupEnum::ByVal { group, .. } => group[n as usize].0.clone(),
        }
    }

    pub fn iter<'a>(&'a self) -> GroupIter<'a, V> {
        GroupIter::new(self)
    }

    pub fn val_iter<'a>(&'a self) -> GroupValIter<'a, V> {
        GroupValIter::new(self)
    }

    fn nth(&self, n: std_usize) -> Option<V> {
        match self {
            GroupEnum::ByRef { group, project, .. } => {
                if self.size() > n {
                    Option::Some {
                        x: project(group[n as usize].0),
                    }
                } else {
                    Option::None
                }
            }
            GroupEnum::ByVal { group, .. } => {
                if self.size() > n {
                    Option::Some {
                        x: group[n as usize].0.clone(),
                    }
                } else {
                    Option::None
                }
            }
        }
    }
}

impl<K, V: Clone> IntoIterator for Group<K, V> {
    type Item = tuple2<V, DDWeight>;
    type IntoIter = GroupIntoIter<V>;

    fn into_iter(self) -> Self::IntoIter {
        GroupIntoIter::new(self)
    }
}

/*
 * DDlog-visible functions.
 */

pub fn group_key<K: Clone, V>(g: &Group<K, V>) -> K {
    g.key()
}

/* Standard aggregation functions. */
pub fn group_count<K, V>(g: &Group<K, V>) -> std_usize {
    g.size()
}

pub fn group_first<K, V: Clone>(g: &Group<K, V>) -> V {
    g.first()
}

pub fn group_nth<K, V: Clone>(g: &Group<K, V>, n: &std_usize) -> Option<V> {
    g.nth(*n)
}

pub fn group_to_set<K, V: Ord + Clone>(g: &Group<K, V>) -> Set<V> {
    let mut res = Set::new();
    for v in g.val_iter() {
        set_insert(&mut res, &v);
    }
    res
}

pub fn group_set_unions<K, V: Ord + Clone>(g: &Group<K, Set<V>>) -> Set<V> {
    let mut res = Set::new();
    for gr in g.val_iter() {
        for v in gr.iter() {
            set_insert(&mut res, v);
        }
    }
    res
}

pub fn group_setref_unions<K, V: Ord + Clone>(g: &Group<K, Ref<Set<V>>>) -> Ref<Set<V>> {
    if g.size() == 1 {
        g.first()
    } else {
        let mut res: Ref<Set<V>> = ref_new(&Set::new());
        {
            let mut rres = Ref::get_mut(&mut res).unwrap();
            for gr in g.val_iter() {
                for v in gr.iter() {
                    set_insert(&mut rres, &v);
                }
            }
        }

        res
    }
}

pub fn group_to_vec<K, V: Ord + Clone>(g: &Group<K, V>) -> Vec<V> {
    let mut res = Vec::with_capacity(g.size() as usize);
    for v in g.val_iter() {
        vec_push(&mut res, &v);
    }
    res
}

pub fn group_to_map<K1, K2: Ord + Clone, V: Clone>(g: &Group<K1, tuple2<K2, V>>) -> Map<K2, V> {
    let mut res = Map::new();
    for tuple2(k, v) in g.val_iter() {
        map_insert(&mut res, &k, &v);
    }
    res
}

pub fn group_to_setmap<K1, K2: Ord + Clone, V: Clone + Ord>(
    g: &Group<K1, tuple2<K2, V>>,
) -> Map<K2, Set<V>> {
    let mut res = Map::new();
    for tuple2(k, v) in g.val_iter() {
        match res.x.entry(k) {
            btree_map::Entry::Vacant(ve) => {
                ve.insert(set_singleton(&v));
            }
            btree_map::Entry::Occupied(mut oe) => {
                oe.get_mut().insert(v);
            }
        }
    }

    res
}

pub fn group_min<K, V: Clone + Ord>(g: &Group<K, V>) -> V {
    g.val_iter().min().unwrap()
}

pub fn group_max<K, V: Clone + Ord>(g: &Group<K, V>) -> V {
    g.val_iter().max().unwrap()
}

pub fn group_sum<K, V: Clone + ops::Add<Output = V>>(g: &Group<K, V>) -> V {
    let mut res = group_first(g);
    for v in g.val_iter().skip(1) {
        res = res + v;
    }

    res
}

/* Tuples */
#[derive(Copy, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct tuple0;

impl Debug for tuple0 {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_tuple("").finish()
    }
}

impl From<()> for tuple0 {
    fn from(_: ()) -> Self {
        Self
    }
}

impl Into<()> for tuple0 {
    fn into(self) {}
}

impl FromRecord for tuple0 {
    fn from_record(val: &Record) -> StdResult<Self, String> {
        <()>::from_record(val).map(|_| tuple0)
    }
}

impl IntoRecord for tuple0 {
    fn into_record(self) -> Record {
        ().into_record()
    }
}

impl Abomonation for tuple0 {}

macro_rules! declare_tuples {
    (
        $(
            $tuple_name:ident<$($element:tt),* $(,)?>
        ),*
        $(,)?
    ) => {
        $(
            #[derive(Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
            pub struct $tuple_name<$($element,)*>($(pub $element,)*);

            impl<$($element),*> From<($($element,)*)> for $tuple_name<$($element,)*> {
                fn from(($($element,)*): ($($element,)*)) -> Self {
                    Self($($element),*)
                }
            }

            impl<$($element),*> Into<($($element,)*)> for $tuple_name<$($element,)*> {
                fn into(self) -> ($($element,)*) {
                    let $tuple_name($($element),*) = self;
                    ($($element,)*)
                }
            }

            impl<$($element: Debug),*> Debug for $tuple_name<$($element),*> {
                fn fmt(&self, f: &mut Formatter) -> FmtResult {
                    let $tuple_name($($element),*) = self;
                    f.debug_tuple("")
                        $(.field(&$element))*
                        .finish()
                }
            }

            impl<$($element: Copy),*> Copy for $tuple_name<$($element),*> {}

            impl<$($element),*> Abomonation for $tuple_name<$($element),*> {}

            impl<$($element: FromRecord),*> FromRecord for $tuple_name<$($element),*> {
                fn from_record(val: &Record) -> StdResult<Self, String> {
                    <($($element,)*)>::from_record(val).map(|($($element,)*)| {
                        $tuple_name($($element),*)
                    })
                }
            }

            impl<$($element: IntoRecord),*> IntoRecord for $tuple_name<$($element),*> {
                fn into_record(self) -> Record {
                    let $tuple_name($($element),*) = self;
                    Record::Tuple(vec![$($element.into_record()),*])
                }
            }

            impl<$($element: FromRecord),*> Mutator<$tuple_name<$($element),*>> for Record {
                fn mutate(&self, x: &mut $tuple_name<$($element),*>) -> StdResult<(), String> {
                    *x = <$tuple_name<$($element),*>>::from_record(self)?;
                    Ok(())
                }
            }
        )*
    };
}

declare_tuples! {
    tuple1 <T1>,
    tuple2 <T1, T2>,
    tuple3 <T1, T2, T3>,
    tuple4 <T1, T2, T3, T4>,
    tuple5 <T1, T2, T3, T4, T5>,
    tuple6 <T1, T2, T3, T4, T5, T6>,
    tuple7 <T1, T2, T3, T4, T5, T6, T7>,
    tuple8 <T1, T2, T3, T4, T5, T6, T7, T8>,
    tuple9 <T1, T2, T3, T4, T5, T6, T7, T8, T9>,
    tuple10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>,
    tuple11<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>,
    tuple12<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>,
    tuple13<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>,
    tuple14<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>,
    tuple15<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>,
    tuple16<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>,
    tuple17<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>,
    tuple18<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>,
    tuple19<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>,
    tuple20<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>,
    tuple21<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>,
    tuple22<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>,
    tuple23<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>,
    tuple24<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>,
    tuple25<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24, T25>,
    tuple26<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24, T25, T26>,
    tuple27<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24, T25, T26, T27>,
    tuple28<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24, T25, T26, T27, T28>,
    tuple29<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29>,
    tuple30<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T30>,
}

// Endianness
pub fn ntohl(x: &u32) -> u32 {
    u32::from_be(*x)
}

pub fn ntohs(x: &u16) -> u16 {
    u16::from_be(*x)
}

pub fn htonl(x: &u32) -> u32 {
    u32::to_be(*x)
}

pub fn htons(x: &u16) -> u16 {
    u16::to_be(*x)
}
