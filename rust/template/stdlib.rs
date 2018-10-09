//! Rust implementation of DDlog standard library functions and types.

#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals, unused_parens, non_shorthand_field_patterns, dead_code)]

use differential_datalog::arcval;
use cmd_parser::*;
use abomonation::Abomonation;

use std::fmt::Display;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use twox_hash::XxHash;
use std::vec;
use std::collections::btree_set;
use std::collections::btree_map;
use std::vec::{Vec};
use std::collections::{BTreeMap, BTreeSet};

const XX_SEED1: u64 = 0x23b691a751d0e108;
const XX_SEED2: u64 = 0x20b09801dce5ff84;

// Vector

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct std_Vec<T> {
    pub x: Vec<T>
}

impl <T: Ord> std_Vec<T> {
    pub fn new() -> Self {
        std_Vec{x: Vec::new()}
    }
    pub fn push(&mut self, v: T) {
        self.x.push(v);
    }
}

impl<T: FromRecord> FromRecord for std_Vec<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        Vec::from_record(val).map(|x|std_Vec{x})
    }
}

impl<T: Display> Display for std_Vec<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, v) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("{}", *v))?;
            if i < len-1 {
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

// Set

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct std_Set<T: Ord> {
    pub x: BTreeSet<T>
}

impl <T: Ord> std_Set<T> {
    pub fn new() -> Self {
        std_Set{x: BTreeSet::new()}
    }
    pub fn insert(&mut self, v: T) {
        self.x.insert(v);
    }
}

impl<T: FromRecord + Ord> FromRecord for std_Set<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        BTreeSet::from_record(val).map(|x|std_Set{x})
    }
}

impl<T: Ord> IntoIterator for std_Set<T> {
    type Item = T;
    type IntoIter = btree_set::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.x.into_iter()
    }
}

impl<T: Display + Ord> Display for std_Set<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, v) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("{}", *v))?;
            if i < len-1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

// Map

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct std_Map<K: Ord,V> {
    pub x: BTreeMap<K,V>
}

impl<K: FromRecord+Ord, V: FromRecord> FromRecord for std_Map<K,V> {
    fn from_record(val: &Record) -> Result<Self, String> {
        BTreeMap::from_record(val).map(|x|std_Map{x})
    }
}

impl<K: Ord,V> IntoIterator for std_Map<K,V> {
    type Item = (K,V);
    type IntoIter = btree_map::IntoIter<K,V>;
    fn into_iter(self) -> Self::IntoIter {
        self.x.into_iter()
    }
}


impl<K: Display+Ord, V: Display> Display for std_Map<K,V> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, (k,v)) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("({},{})", *k, *v))?;
            if i < len-1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

// string conversion

pub fn std___builtin_2string<T: Display>(x: &T) -> arcval::DDString {
    arcval::DDString::from(format!("{}", *x).to_string())
}

pub fn std_hex<T: fmt::LowerHex>(x: &T) -> arcval::DDString {
    arcval::DDString::from(format!("{:x}", *x).to_string())
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

/*
 * Group trait (used in aggregation operators)
 */
pub trait Group<X> {
    fn size(&self) -> u64;
    fn ith(&self, i: u64) -> X;
}

/*
 * Standard aggregation function
 */
pub fn std_count<A, G:Group<A>+?Sized>(g: &G) -> u64 {
    g.size()
}

pub fn std_group2set<A: Ord, G:Group<A>+?Sized>(g: &G) -> std_Set<A> {
    let mut res = std_Set::new();
    for i in 0..g.size() {
        res.insert(g.ith(i));
    };
    res
}

pub fn std_group2vec<A: Ord, G:Group<A>+?Sized>(g: &G) -> std_Vec<A> {
    let mut res = std_Vec::new();
    for i in 0..g.size() {
        res.push(g.ith(i));
    };
    res
}
