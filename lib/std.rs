/// Rust implementation of DDlog standard library functions and types.
extern crate num;

use differential_datalog::arcval;
use differential_datalog::record::*;

use serde::de::Deserialize;
use serde::de::Deserializer;
use serde::ser::Serialize;
use serde::ser::Serializer;
use std::cmp;
use std::collections::btree_map;
use std::collections::btree_set;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;
use std::iter::FromIterator;
use std::ops;
use std::slice;
use std::vec;
use std::vec::Vec;
use twox_hash::XxHash;

#[cfg(feature = "flatbuf")]
use flatbuf::{FBIter, FromFlatBuffer, ToFlatBuffer, ToFlatBufferTable, ToFlatBufferVectorElement};

/* FlatBuffers runtime */
#[cfg(feature = "flatbuf")]
use flatbuffers as fbrt;

const XX_SEED1: u64 = 0x23b691a751d0e108;
const XX_SEED2: u64 = 0x20b09801dce5ff84;

// Result

/* Convert Rust result type to DDlog's std.Result. */
pub fn res2std<T, E: Display>(res: Result<T, E>) -> std_Result<T, String> {
    match res {
        Ok(res) => std_Result::std_Ok { res },
        Err(e) => std_Result::std_Err {
            err: format!("{}", e),
        },
    }
}

pub fn std_result_unwrap_or_default<T: Default + Clone, E>(res: &std_Result<T, E>) -> T {
    match res {
        std_Result::std_Ok { res } => res.clone(),
        std_Result::std_Err { err } => T::default(),
    }
}

// Ref
pub type std_Ref<A> = arcval::ArcVal<A>;

pub fn std_ref_new<A: Clone>(x: &A) -> std_Ref<A> {
    arcval::ArcVal::from(x.clone())
}

pub fn std_deref<A: Clone>(x: &std_Ref<A>) -> &A {
    x.deref()
}

#[cfg(feature = "flatbuf")]
impl<T, FB> FromFlatBuffer<FB> for std_Ref<T>
where
    T: FromFlatBuffer<FB>,
{
    fn from_flatbuf(fb: FB) -> Response<Self> {
        Ok(std_Ref::from(T::from_flatbuf(fb)?))
    }
}

#[cfg(feature = "flatbuf")]
impl<'b, T> ToFlatBuffer<'b> for std_Ref<T>
where
    T: ToFlatBuffer<'b>,
{
    type Target = T::Target;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.deref().to_flatbuf(fbb)
    }
}

#[cfg(feature = "flatbuf")]
impl<'b, T> ToFlatBufferTable<'b> for std_Ref<T>
where
    T: ToFlatBufferTable<'b>,
{
    type Target = T::Target;

    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        self.deref().to_flatbuf_table(fbb)
    }
}

#[cfg(feature = "flatbuf")]
impl<'b, T> ToFlatBufferVectorElement<'b> for std_Ref<T>
where
    T: ToFlatBufferVectorElement<'b>,
{
    type Target = T::Target;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.deref().to_flatbuf_vector_element(fbb)
    }
}

// Arithmetic functions
pub fn std_pow32<T: num::One + ops::Mul + Clone>(base: &T, exp: &u32) -> T {
    num::pow::pow(base.clone(), *exp as usize)
}

// Option
pub fn option2std<T>(x: Option<T>) -> std_Option<T> {
    match x {
        None => std_Option::std_None,
        Some(v) => std_Option::std_Some { x: v },
    }
}

pub fn std2option<T>(x: std_Option<T>) -> Option<T> {
    match x {
        std_Option::std_None => None,
        std_Option::std_Some { x } => Some(x),
    }
}

impl<T> From<Option<T>> for std_Option<T> {
    fn from(x: Option<T>) -> Self {
        option2std(x)
    }
}

// this requires Rust 1.41+
impl<T> From<std_Option<T>> for Option<T> {
    fn from(x: std_Option<T>) -> Self {
        std2option(x)
    }
}

pub fn std_option_unwrap_or_default<T: Default + Clone>(opt: &std_Option<T>) -> T {
    match opt {
        std_Option::std_Some { x } => x.clone(),
        std_Option::std_None => T::default(),
    }
}

// Range
pub fn std_range<A: Clone + Ord + ops::Add<Output = A> + PartialOrd>(
    from: &A,
    to: &A,
    step: &A,
) -> std_Vec<A> {
    let mut vec = std_Vec::new();
    let mut x = from.clone();
    while x <= *to {
        vec.push(x.clone());
        x = x + step.clone();
    }
    vec
}

// Vector

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Default)]
pub struct std_Vec<T> {
    pub x: Vec<T>,
}

impl<T: Serialize> Serialize for std_Vec<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.x.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for std_Vec<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::deserialize(deserializer).map(|x| std_Vec { x })
    }
}

/* This is needed so we can support for-loops over `Vec`'s
 */
pub struct VecIter<'a, X> {
    iter: slice::Iter<'a, X>,
}

impl<'a, X> VecIter<'a, X> {
    pub fn new(vec: &'a std_Vec<X>) -> VecIter<'a, X> {
        VecIter { iter: vec.x.iter() }
    }
}

impl<'a, X> Iterator for VecIter<'a, X> {
    type Item = &'a X;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, T> std_Vec<T> {
    pub fn iter(&'a self) -> VecIter<'a, T> {
        VecIter::new(self)
    }
}

impl<T> std_Vec<T> {
    pub fn new() -> Self {
        std_Vec { x: Vec::new() }
    }
    pub fn with_capacity(capacity: usize) -> Self {
        std_Vec {
            x: Vec::with_capacity(capacity),
        }
    }
    pub fn push(&mut self, v: T) {
        self.x.push(v);
    }
}

impl<T: Clone> From<&[T]> for std_Vec<T> {
    fn from(s: &[T]) -> Self {
        std_Vec { x: Vec::from(s) }
    }
}

impl<T: Clone> From<Vec<T>> for std_Vec<T> {
    fn from(x: Vec<T>) -> Self {
        std_Vec { x }
    }
}

impl<T> Deref for std_Vec<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        self.x.deref()
    }
}

impl<T: Clone> std_Vec<T> {
    pub fn extend_from_slice(&mut self, other: &[T]) {
        self.x.extend_from_slice(other);
    }
    pub fn resize(&mut self, new_len: usize, value: T) {
        self.x.resize(new_len, value);
    }
}

impl<T: FromRecord> FromRecord for std_Vec<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        Vec::from_record(val).map(|x| std_Vec { x })
    }
}

impl<T: IntoRecord> IntoRecord for std_Vec<T> {
    fn into_record(self) -> Record {
        self.x.into_record()
    }
}

impl<T: FromRecord> Mutator<std_Vec<T>> for Record {
    fn mutate(&self, vec: &mut std_Vec<T>) -> Result<(), String> {
        self.mutate(&mut vec.x)
    }
}

impl<T: Display> Display for std_Vec<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, v) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("{}", *v))?;
            if i < len - 1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

impl<T: fmt::Debug> fmt::Debug for std_Vec<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, v) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("{:?}", *v))?;
            if i < len - 1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

impl<T> IntoIterator for std_Vec<T> {
    type Item = T;
    type IntoIter = vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.x.into_iter()
    }
}

#[cfg(feature = "flatbuf")]
impl<'a, T, F> FromFlatBuffer<fbrt::Vector<'a, F>> for std_Vec<T>
where
    T: Ord + FromFlatBuffer<F::Inner>,
    F: fbrt::Follow<'a> + 'a,
{
    fn from_flatbuf(fb: fbrt::Vector<'a, F>) -> Response<Self> {
        let mut vec = std_Vec::with_capacity(fb.len());
        for x in FBIter::from_vector(fb) {
            vec.push(T::from_flatbuf(x)?);
        }
        Ok(vec)
    }
}

// For scalar types, the FlatBuffers API returns slice instead of 'Vector'.
#[cfg(feature = "flatbuf")]
impl<'a, T> FromFlatBuffer<&'a [T]> for std_Vec<T>
where
    T: Clone,
{
    fn from_flatbuf(fb: &'a [T]) -> Response<Self> {
        let mut vec = std_Vec::with_capacity(fb.len());
        vec.extend_from_slice(fb);
        Ok(vec)
    }
}

#[cfg(feature = "flatbuf")]
impl<'b, T> ToFlatBuffer<'b> for std_Vec<T>
where
    T: ToFlatBufferVectorElement<'b>,
{
    type Target = fbrt::WIPOffset<fbrt::Vector<'b, <T::Target as fbrt::Push>::Output>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec: Vec<T::Target> = self
            .iter()
            .map(|x| x.to_flatbuf_vector_element(fbb))
            .collect();
        fbb.create_vector(vec.as_slice())
    }
}

pub fn std_vec_len<X: Ord + Clone>(v: &std_Vec<X>) -> std_usize {
    v.x.len() as std_usize
}

pub fn std_vec_empty<X: Ord + Clone>() -> std_Vec<X> {
    std_Vec::new()
}

pub fn std_vec_with_length<X: Ord + Clone>(len: &std_usize, x: &X) -> std_Vec<X> {
    let mut res = std_Vec::with_capacity(*len as usize);
    res.resize(*len as usize, x.clone());
    res
}

pub fn std_vec_with_capacity<X: Ord + Clone>(len: &std_usize) -> std_Vec<X> {
    std_Vec::with_capacity(*len as usize)
}

pub fn std_vec_singleton<X: Ord + Clone>(x: &X) -> std_Vec<X> {
    std_Vec { x: vec![x.clone()] }
}

pub fn std_vec_append<X: Ord + Clone>(v: &mut std_Vec<X>, other: &std_Vec<X>) {
    v.extend_from_slice(other.x.as_slice());
}

pub fn std_vec_push<X: Ord + Clone>(v: &mut std_Vec<X>, x: &X) {
    v.push((*x).clone());
}

pub fn std_vec_push_imm<X: Ord + Clone>(v: &std_Vec<X>, x: &X) -> std_Vec<X> {
    let mut v2 = v.clone();
    v2.push((*x).clone());
    v2
}

pub fn std_vec_contains<X: Ord>(v: &std_Vec<X>, x: &X) -> bool {
    v.x.contains(x)
}

pub fn std_vec_is_empty<X: Ord>(v: &std_Vec<X>) -> bool {
    v.x.is_empty()
}

pub fn std_vec_nth<X: Ord + Clone>(v: &std_Vec<X>, n: &std_usize) -> std_Option<X> {
    option2std(v.x.get(*n as usize).cloned())
}

pub fn std_vec2set<X: Ord + Clone>(s: &std_Vec<X>) -> std_Set<X> {
    std_Set {
        x: s.x.iter().cloned().collect(),
    }
}

pub fn std_vec_sort<X: Ord>(v: &mut std_Vec<X>) {
    v.x.as_mut_slice().sort();
}

pub fn std_vec_sort_imm<X: Ord + Clone>(v: &std_Vec<X>) -> std_Vec<X> {
    let mut res = (*v).clone();
    res.x.sort();
    res
}

// Set

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Default)]
pub struct std_Set<T: Ord> {
    pub x: BTreeSet<T>,
}

impl<T: Ord + Serialize> Serialize for std_Set<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.x.serialize(serializer)
    }
}

impl<'de, T: Ord + Deserialize<'de>> Deserialize<'de> for std_Set<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeSet::deserialize(deserializer).map(|x| std_Set { x })
    }
}

/* This is needed so we can support for-loops over `Set`'s
 */
pub struct SetIter<'a, X> {
    iter: btree_set::Iter<'a, X>,
}

impl<'a, X: Ord> SetIter<'a, X> {
    pub fn new(set: &'a std_Set<X>) -> SetIter<'a, X> {
        SetIter { iter: set.x.iter() }
    }
}

impl<'a, X> Iterator for SetIter<'a, X> {
    type Item = &'a X;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, T: Ord> std_Set<T> {
    pub fn iter(&'a self) -> SetIter<'a, T> {
        SetIter::new(self)
    }
}

impl<T: Ord> std_Set<T> {
    pub fn new() -> Self {
        std_Set { x: BTreeSet::new() }
    }
    pub fn insert(&mut self, v: T) {
        self.x.insert(v);
    }
}

impl<T: FromRecord + Ord> FromRecord for std_Set<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        BTreeSet::from_record(val).map(|x| std_Set { x })
    }
}

impl<T: IntoRecord + Ord> IntoRecord for std_Set<T> {
    fn into_record(self) -> Record {
        self.x.into_record()
    }
}

impl<T: FromRecord + Ord> Mutator<std_Set<T>> for Record {
    fn mutate(&self, set: &mut std_Set<T>) -> Result<(), String> {
        self.mutate(&mut set.x)
    }
}

impl<T: Ord> IntoIterator for std_Set<T> {
    type Item = T;
    type IntoIter = btree_set::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.x.into_iter()
    }
}

impl<T: Ord> FromIterator<T> for std_Set<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        std_Set {
            x: BTreeSet::from_iter(iter),
        }
    }
}

impl<T: Display + Ord> Display for std_Set<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, v) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("{}", *v))?;
            if i < len - 1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

impl<T: fmt::Debug + Ord> fmt::Debug for std_Set<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, v) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("{:?}", *v))?;
            if i < len - 1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

#[cfg(feature = "flatbuf")]
impl<'a, T, F> FromFlatBuffer<fbrt::Vector<'a, F>> for std_Set<T>
where
    T: Ord + FromFlatBuffer<F::Inner>,
    F: fbrt::Follow<'a> + 'a,
{
    fn from_flatbuf(fb: fbrt::Vector<'a, F>) -> Response<Self> {
        let mut set = std_Set::new();
        for x in FBIter::from_vector(fb) {
            set.insert(T::from_flatbuf(x)?);
        }
        Ok(set)
    }
}

// For scalar types, the FlatBuffers API returns slice instead of 'Vector'.
#[cfg(feature = "flatbuf")]
impl<'a, T> FromFlatBuffer<&'a [T]> for std_Set<T>
where
    T: Ord + Clone,
{
    fn from_flatbuf(fb: &'a [T]) -> Response<Self> {
        let mut set = std_Set::new();
        for x in fb.iter() {
            set.insert(x.clone());
        }
        Ok(set)
    }
}

#[cfg(feature = "flatbuf")]
impl<'b, T> ToFlatBuffer<'b> for std_Set<T>
where
    T: Ord + ToFlatBufferVectorElement<'b>,
{
    type Target = fbrt::WIPOffset<fbrt::Vector<'b, <T::Target as fbrt::Push>::Output>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec: Vec<T::Target> = self
            .iter()
            .map(|x| x.to_flatbuf_vector_element(fbb))
            .collect();
        fbb.create_vector(vec.as_slice())
    }
}

pub fn std_set_size<X: Ord + Clone>(s: &std_Set<X>) -> std_usize {
    s.x.len() as std_usize
}

pub fn std_set_empty<X: Ord + Clone>() -> std_Set<X> {
    std_Set::new()
}

pub fn std_set_singleton<X: Ord + Clone>(v: &X) -> std_Set<X> {
    let mut s = std_Set::new();
    s.insert(v.clone());
    s
}

pub fn std_set_insert<X: Ord + Clone>(s: &mut std_Set<X>, v: &X) {
    s.x.insert((*v).clone());
}

pub fn std_set_insert_imm<X: Ord + Clone>(s: &std_Set<X>, v: &X) -> std_Set<X> {
    let mut s2 = s.clone();
    s2.insert((*v).clone());
    s2
}

pub fn std_set_contains<X: Ord>(s: &std_Set<X>, v: &X) -> bool {
    s.x.contains(v)
}

pub fn std_set_is_empty<X: Ord>(s: &std_Set<X>) -> bool {
    s.x.is_empty()
}

pub fn std_set_nth<X: Ord + Clone>(s: &std_Set<X>, n: &std_usize) -> std_Option<X> {
    option2std(s.x.iter().nth(*n as usize).cloned())
}

pub fn std_set2vec<X: Ord + Clone>(s: &std_Set<X>) -> std_Vec<X> {
    std_Vec {
        x: s.x.iter().cloned().collect(),
    }
}

pub fn std_set_union<X: Ord + Clone>(s1: &std_Set<X>, s2: &std_Set<X>) -> std_Set<X> {
    let mut s = s1.clone();
    s.x.append(&mut s2.x.clone());
    s
}

pub fn std_set_unions<X: Ord + Clone>(sets: &std_Vec<std_Set<X>>) -> std_Set<X> {
    let mut s = BTreeSet::new();
    for si in sets.x.iter() {
        s.append(&mut si.x.clone());
    }
    std_Set { x: s }
}

pub fn std_set_intersection<X: Ord + Clone>(s1: &std_Set<X>, s2: &std_Set<X>) -> std_Set<X> {
    std_Set {
        x: s1.x.intersection(&s2.x).cloned().collect(),
    }
}

pub fn std_set_difference<X: Ord + Clone>(s1: &std_Set<X>, s2: &std_Set<X>) -> std_Set<X> {
    std_Set {
        x: s1.x.difference(&s2.x).cloned().collect(),
    }
}

// Map

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Default)]
pub struct std_Map<K: Ord, V> {
    pub x: BTreeMap<K, V>,
}

impl<K: Ord + Serialize, V: Serialize> Serialize for std_Map<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.x.serialize(serializer)
    }
}

impl<'de, K: Ord + Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for std_Map<K, V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeMap::deserialize(deserializer).map(|x| std_Map { x })
    }
}

/* This is needed so we can support for-loops over `Map`'s
 */
pub struct MapIter<'a, K, V> {
    iter: btree_map::Iter<'a, K, V>,
}

impl<'a, K: Ord, V> MapIter<'a, K, V> {
    pub fn new(map: &'a std_Map<K, V>) -> MapIter<'a, K, V> {
        MapIter { iter: map.x.iter() }
    }
}

impl<'a, K: Clone, V: Clone> Iterator for MapIter<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (k.clone(), v.clone()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, K: Ord, V> std_Map<K, V> {
    pub fn iter(&'a self) -> MapIter<'a, K, V> {
        MapIter::new(self)
    }
}

impl<K: Ord, V> std_Map<K, V> {
    pub fn new() -> Self {
        std_Map { x: BTreeMap::new() }
    }
    pub fn insert(&mut self, k: K, v: V) {
        self.x.insert(k, v);
    }
}

impl<K: FromRecord + Ord, V: FromRecord> FromRecord for std_Map<K, V> {
    fn from_record(val: &Record) -> Result<Self, String> {
        BTreeMap::from_record(val).map(|x| std_Map { x })
    }
}

impl<K: IntoRecord + Ord, V: IntoRecord> IntoRecord for std_Map<K, V> {
    fn into_record(self) -> Record {
        self.x.into_record()
    }
}

impl<K: FromRecord + Ord, V: FromRecord + PartialEq> Mutator<std_Map<K, V>> for Record {
    fn mutate(&self, map: &mut std_Map<K, V>) -> Result<(), String> {
        self.mutate(&mut map.x)
    }
}

impl<K: Ord, V> IntoIterator for std_Map<K, V> {
    type Item = (K, V);
    type IntoIter = btree_map::IntoIter<K, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.x.into_iter()
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for std_Map<K, V> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        std_Map {
            x: BTreeMap::from_iter(iter),
        }
    }
}

impl<K: Display + Ord, V: Display> Display for std_Map<K, V> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, (k, v)) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("({},{})", *k, *v))?;
            if i < len - 1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

impl<K: fmt::Debug + Ord, V: fmt::Debug> fmt::Debug for std_Map<K, V> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, (k, v)) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("({:?},{:?})", *k, *v))?;
            if i < len - 1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

#[cfg(feature = "flatbuf")]
impl<'a, K, V, F> FromFlatBuffer<fbrt::Vector<'a, F>> for std_Map<K, V>
where
    F: fbrt::Follow<'a> + 'a,
    K: Ord,
    (K, V): FromFlatBuffer<F::Inner>,
{
    fn from_flatbuf(fb: fbrt::Vector<'a, F>) -> Response<Self> {
        let mut m = std_Map::new();
        for x in FBIter::from_vector(fb) {
            let (k, v) = <(K, V)>::from_flatbuf(x)?;
            m.insert(k, v);
        }
        Ok(m)
    }
}

#[cfg(feature = "flatbuf")]
impl<'b, K, V, T> ToFlatBuffer<'b> for std_Map<K, V>
where
    K: Ord + Clone,
    V: Clone,
    (K, V): ToFlatBufferVectorElement<'b, Target = T>,
    T: 'b + fbrt::Push + Copy,
{
    type Target = fbrt::WIPOffset<fbrt::Vector<'b, <T as fbrt::Push>::Output>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec: Vec<<(K, V) as ToFlatBufferVectorElement<'b>>::Target> = self
            .iter()
            .map(|(k, v)| (k, v).to_flatbuf_vector_element(fbb))
            .collect();
        fbb.create_vector(vec.as_slice())
    }
}

pub fn std_map_size<K: Ord, V>(m: &std_Map<K, V>) -> std_usize {
    m.x.len() as std_usize
}

pub fn std_map_empty<K: Ord + Clone, V: Clone>() -> std_Map<K, V> {
    std_Map::new()
}

pub fn std_map_singleton<K: Ord + Clone, V: Clone>(k: &K, v: &V) -> std_Map<K, V> {
    let mut m = std_Map::new();
    m.insert(k.clone(), v.clone());
    m
}

pub fn std_map_insert<K: Ord + Clone, V: Clone>(m: &mut std_Map<K, V>, k: &K, v: &V) {
    m.x.insert((*k).clone(), (*v).clone());
}

pub fn std_map_remove<K: Ord + Clone, V: Clone>(m: &mut std_Map<K, V>, k: &K) {
    m.x.remove(k);
}

pub fn std_map_insert_imm<K: Ord + Clone, V: Clone>(
    m: &std_Map<K, V>,
    k: &K,
    v: &V,
) -> std_Map<K, V> {
    let mut m2 = m.clone();
    m2.insert((*k).clone(), (*v).clone());
    m2
}

pub fn std_map_get<K: Ord, V: Clone>(m: &std_Map<K, V>, k: &K) -> std_Option<V> {
    option2std(m.x.get(k).cloned())
}

pub fn std_map_contains_key<K: Ord, V: Clone>(s: &std_Map<K, V>, k: &K) -> bool {
    s.x.contains_key(k)
}

pub fn std_map_is_empty<K: Ord, V: Clone>(m: &std_Map<K, V>) -> bool {
    m.x.is_empty()
}

pub fn std_map_union<K: Ord + Clone, V: Clone>(
    m1: &std_Map<K, V>,
    m2: &std_Map<K, V>,
) -> std_Map<K, V> {
    let mut m = m1.clone();
    m.x.append(&mut m2.x.clone());
    m
}

// strings

pub fn std___builtin_2string<T: Display>(x: &T) -> String {
    format!("{}", *x).to_string()
}

pub fn std_hex<T: fmt::LowerHex>(x: &T) -> String {
    format!("{:x}", *x).to_string()
}

pub fn std_parse_dec_u64(s: &String) -> std_Option<u64> {
    option2std(s.parse::<u64>().ok())
}

pub fn std_parse_dec_i64(s: &String) -> std_Option<i64> {
    option2std(s.parse::<i64>().ok())
}

pub fn std_string_join(strings: &std_Vec<String>, sep: &String) -> String {
    strings.x.join(sep.as_str())
}

pub fn std_string_split(s: &String, sep: &String) -> std_Vec<String> {
    std_Vec {
        x: s.split(sep).map(|x| x.to_owned()).collect(),
    }
}

pub fn std_string_contains(s1: &String, s2: &String) -> bool {
    s1.contains(s2.as_str())
}

pub fn std_string_substr(s: &String, start: &std_usize, end: &std_usize) -> String {
    let len = s.len();
    let from = cmp::min(*start as usize, len);
    let to = cmp::max(from, cmp::min(*end as usize, len));
    s[from..to].to_string()
}

pub fn std_string_replace(s: &String, from: &String, to: &String) -> String {
    s.replace(from, to)
}

pub fn std_string_starts_with(s: &String, prefix: &String) -> bool {
    s.starts_with(prefix)
}

pub fn std_string_ends_with(s: &String, suffix: &String) -> bool {
    s.ends_with(suffix)
}

pub fn std_string_trim(s: &String) -> String {
    s.trim().to_string()
}

pub fn std_string_len(s: &String) -> std_usize {
    s.len() as std_usize
}

pub fn std_string_to_bytes(s: &String) -> std_Vec<u8> {
    std_Vec::from(s.as_bytes())
}

pub fn std_str_to_lower(s: &String) -> String {
    s.to_lowercase()
}

pub fn std_string_to_lowercase(s: &String) -> String {
    s.to_lowercase()
}

pub fn std_string_to_uppercase(s: &String) -> String {
    s.to_uppercase()
}

pub fn std_string_reverse(s: &String) -> String {
    s.chars().rev().collect()
}

// Hashing

pub fn std_hash64<T: Hash>(x: &T) -> u64 {
    let mut hasher = XxHash::with_seed(XX_SEED1);
    x.hash(&mut hasher);
    hasher.finish()
}

pub fn std_hash128<T: Hash>(x: &T) -> u128 {
    let mut hasher = XxHash::with_seed(XX_SEED1);
    x.hash(&mut hasher);
    let w1 = hasher.finish();
    let mut hasher = XxHash::with_seed(XX_SEED2);
    x.hash(&mut hasher);
    let w2 = hasher.finish();
    ((w1 as u128) << 64) | (w2 as u128)
}

pub type ProjectFunc<X> = fn(&DDValue) -> X;

/*
 * Group type (used in aggregation operators)
 */
pub struct std_Group<'a, K, V> {
    /* TODO: remove "pub" */
    pub key: &'a K,
    pub group: &'a [(&'a DDValue, Weight)],
    pub project: &'a ProjectFunc<V>,
}

/* This is needed so we can support for-loops over `Group`'s
 */
pub struct GroupIter<'a, V> {
    iter: slice::Iter<'a, (&'a DDValue, Weight)>,
    project: &'a ProjectFunc<V>,
}

impl<'a, V> GroupIter<'a, V> {
    pub fn new<K>(grp: &std_Group<'a, K, V>) -> GroupIter<'a, V> {
        GroupIter {
            iter: grp.group.iter(),
            project: grp.project,
        }
    }
}

impl<'a, V> Iterator for GroupIter<'a, V> {
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some((x, _)) => Some((self.project)(x)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, K: Clone, V> std_Group<'a, K, V> {
    fn key(&self) -> K {
        self.key.clone()
    }
}

impl<'a, K, V> std_Group<'a, K, V> {
    pub fn new(
        key: &'a K,
        group: &'a [(&'a DDValue, Weight)],
        project: &'static ProjectFunc<V>,
    ) -> std_Group<'a, K, V> {
        std_Group {
            key,
            group,
            project,
        }
    }

    fn size(&self) -> std_usize {
        self.group.len() as std_usize
    }

    fn first(&'a self) -> V {
        (self.project)(self.group[0].0)
    }

    fn nth_unchecked(&'a self, n: std_usize) -> V {
        (self.project)(self.group[n as usize].0)
    }

    pub fn iter(&'a self) -> GroupIter<'a, V> {
        GroupIter::new(self)
    }
}

impl<'a, K, V> std_Group<'a, K, V> {
    fn nth(&'a self, n: std_usize) -> std_Option<V> {
        if self.size() > n {
            std_Option::std_Some {
                x: (self.project)(self.group[n as usize].0),
            }
        } else {
            std_Option::std_None
        }
    }
}

pub fn std_group_key<K: Clone, V>(g: &std_Group<K, V>) -> K {
    g.key()
}

/*
 * Standard aggregation functions
 */
pub fn std_group_count<K, V>(g: &std_Group<K, V>) -> std_usize {
    g.size()
}

pub fn std_group_first<K, V>(g: &std_Group<K, V>) -> V {
    g.first()
}

pub fn std_group_nth<K, V>(g: &std_Group<K, V>, n: &std_usize) -> std_Option<V> {
    g.nth(*n)
}

pub fn std_group2set<K, V: Ord + Clone>(g: &std_Group<K, V>) -> std_Set<V> {
    let mut res = std_Set::new();
    for v in g.iter() {
        std_set_insert(&mut res, &v);
    }
    res
}

pub fn std_group_set_unions<K, V: Ord + Clone>(g: &std_Group<K, std_Set<V>>) -> std_Set<V> {
    let mut res = std_Set::new();
    for gr in g.iter() {
        for v in gr.iter() {
            std_set_insert(&mut res, v);
        }
    }
    res
}

pub fn std_group_setref_unions<K, V: Ord + Clone>(
    g: &std_Group<K, std_Ref<std_Set<V>>>,
) -> std_Ref<std_Set<V>> {
    if g.size() == 1 {
        g.first()
    } else {
        let mut res: std_Ref<std_Set<V>> = std_ref_new(&std_Set::new());
        {
            let mut rres = std_Ref::get_mut(&mut res).unwrap();
            for gr in g.iter() {
                for v in gr.iter() {
                    std_set_insert(&mut rres, &v);
                }
            }
        }
        res
    }
}

pub fn std_group2vec<K, V: Ord + Clone>(g: &std_Group<K, V>) -> std_Vec<V> {
    let mut res = std_Vec::with_capacity(g.size() as usize);
    for v in g.iter() {
        std_vec_push(&mut res, &v);
    }
    res
}

pub fn std_group2map<K1, K2: Ord + Clone, V: Clone>(g: &std_Group<K1, (K2, V)>) -> std_Map<K2, V> {
    let mut res = std_Map::new();
    for (k, v) in g.iter() {
        std_map_insert(&mut res, &k, &v);
    }
    res
}

pub fn std_group2setmap<K1, K2: Ord + Clone, V: Clone + Ord>(
    g: &std_Group<K1, (K2, V)>,
) -> std_Map<K2, std_Set<V>> {
    let mut res = std_Map::new();
    for (k, v) in g.iter() {
        match res.x.entry(k) {
            btree_map::Entry::Vacant(ve) => {
                ve.insert(std_set_singleton(&v));
            }
            btree_map::Entry::Occupied(mut oe) => {
                oe.get_mut().insert(v);
            }
        }
    }
    res
}

pub fn std_group_min<K, V: Ord>(g: &std_Group<K, V>) -> V {
    g.iter().min().unwrap()
}

pub fn std_group_max<K, V: Ord>(g: &std_Group<K, V>) -> V {
    g.iter().max().unwrap()
}

pub fn std_group_sum<K, V: ops::Add + ops::AddAssign>(g: &std_Group<K, V>) -> V {
    let mut res = std_group_first(g);
    for v in g.iter().skip(1) {
        res += v;
    }
    res
}

/* Tuples */
#[derive(Copy, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct tuple0;

impl FromRecord for tuple0 {
    fn from_record(val: &Record) -> Result<Self, String> {
        <()>::from_record(val).map(|_| tuple0)
    }
}

impl IntoRecord for tuple0 {
    fn into_record(self) -> Record {
        ().into_record()
    }
}

macro_rules! decl_tuple {
    ( $name:ident, $( $t:tt ),+ ) => {
        #[derive(Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
        pub struct $name< $($t),* >($(pub $t),*);
        impl <$($t: FromRecord),*> FromRecord for $name<$($t),*> {
            fn from_record(val: &Record) -> Result<Self, String> {
                <($($t),*)>::from_record(val).map(|($($t),*)|$name($($t),*))
            }
        }

        impl <$($t: IntoRecord),*> IntoRecord for $name<$($t),*> {
            fn into_record(self) -> Record {
                let $name($($t),*) = self;
                Record::Tuple(vec![$($t.into_record()),*])
            }
        }

        impl <$($t: FromRecord),*> Mutator<$name<$($t),*>> for Record {
            fn mutate(&self, x: &mut $name<$($t),*>) -> Result<(), String> {
                *x = <$name<$($t),*>>::from_record(self)?;
                Ok(())
            }
        }
    };
}

decl_tuple!(tuple2, T1, T2);
decl_tuple!(tuple3, T1, T2, T3);
decl_tuple!(tuple4, T1, T2, T3, T4);
decl_tuple!(tuple5, T1, T2, T3, T4, T5);
decl_tuple!(tuple6, T1, T2, T3, T4, T5, T6);
decl_tuple!(tuple7, T1, T2, T3, T4, T5, T6, T7);
decl_tuple!(tuple8, T1, T2, T3, T4, T5, T6, T7, T8);
decl_tuple!(tuple9, T1, T2, T3, T4, T5, T6, T7, T8, T9);
decl_tuple!(tuple10, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
decl_tuple!(tuple11, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
decl_tuple!(tuple12, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
decl_tuple!(tuple13, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
decl_tuple!(tuple14, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
decl_tuple!(tuple15, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
decl_tuple!(tuple16, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
decl_tuple!(tuple17, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17);
decl_tuple!(
    tuple18, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18
);
decl_tuple!(
    tuple19, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19
);
decl_tuple!(
    tuple20, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19,
    T20
);

// Endianness
pub fn std_ntohl(x: &u32) -> u32 {
    u32::from_be(*x)
}

pub fn std_ntohs(x: &u16) -> u16 {
    u16::from_be(*x)
}

pub fn std_htonl(x: &u32) -> u32 {
    u32::to_be(*x)
}

pub fn std_htons(x: &u16) -> u16 {
    u16::to_be(*x)
}
