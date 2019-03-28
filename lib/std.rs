/// Rust implementation of DDlog standard library functions and types.

use differential_datalog::arcval;
use differential_datalog::record::*;

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
use std::iter::FromIterator;
use std::ops;
use std::cmp;

const XX_SEED1: u64 = 0x23b691a751d0e108;
const XX_SEED2: u64 = 0x20b09801dce5ff84;

// Ref
pub type std_Ref<A> = arcval::ArcVal<A>;

pub fn std_ref_new<A: Clone>(x: &A) -> std_Ref<A> {
    arcval::ArcVal::from(x.clone())
}

pub fn std_deref<A: Clone>(x: &std_Ref<A>) -> &A {
    x.deref()
}

// min/max
pub fn std_max<A: Ord + Clone>(x: &A, y: &A) -> A {
    if *x >= *y {
        x.clone()
    } else {
        y.clone()
    }
}

pub fn std_min<A: Ord + Clone>(x: &A, y: &A) -> A {
    if *x <= *y {
        x.clone()
    } else {
        y.clone()
    }
}

// Option
pub fn option2std<T: Clone>(x: Option<T>) -> std_Option<T> {
    match x {
        None => std_Option::std_None,
        Some(v) => std_Option::std_Some{x: v}
    }
}

// Range
pub fn std_range<A: Clone + Ord + ops::Add<Output = A> + PartialOrd>(from: &A, to: &A, step: &A) -> std_Vec<A> {
    let mut vec = std_Vec::new();
    let mut x = from.clone();
    while x <= *to {
        vec.push(x.clone());
        x = x + step.clone();
    };
    vec
}

// Vector

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug, Default)]
pub struct std_Vec<T> {
    pub x: Vec<T>
}

impl <T: Ord> std_Vec<T> {
    pub fn new() -> Self {
        std_Vec{x: Vec::new()}
    }
    pub fn with_capacity(capacity: usize) -> Self {
        std_Vec{x: Vec::with_capacity(capacity)}
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

impl<T: IntoRecord> IntoRecord for std_Vec<T> {
    fn into_record(self) -> Record {
        self.x.into_record()
    }
}

impl<T: FromRecord> Mutator<std_Vec<T>> for Record
{
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

pub fn std_vec_len<X: Ord + Clone>(v: &std_Vec<X>) -> u64 {
    v.x.len() as u64
}

pub fn std_vec_empty<X: Ord + Clone>() -> std_Vec<X> {
    std_Vec::new()
}

pub fn std_vec_singleton<X: Ord + Clone>(x: &X) -> std_Vec<X> {
    std_Vec{x: vec![x.clone()]}
}

pub fn std_vec_push<X: Ord+Clone>(v: &mut std_Vec<X>, x: &X) {
    v.push((*x).clone());
}

pub fn std_vec_insert_imm<X: Ord+Clone>(v: &std_Vec<X>, x: &X) -> std_Vec<X> {
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

pub fn std_vec_nth<X: Ord + Clone>(v: &std_Vec<X>, n: &u64) -> std_Option<X> {
    option2std(v.x.get(*n as usize).cloned())
}

pub fn std_vec2set<X: Ord + Clone>(s: &std_Vec<X>) -> std_Set<X> {
    std_Set{x: s.x.iter().cloned().collect()}
}

// Set

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug, Default)]
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

impl<T: IntoRecord + Ord> IntoRecord for std_Set<T> {
    fn into_record(self) -> Record {
        self.x.into_record()
    }
}

impl<T: FromRecord + Ord> Mutator<std_Set<T>> for Record
{
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
    where I: IntoIterator<Item = T>
    {
        std_Set{x: BTreeSet::from_iter(iter)}
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

pub fn std_set_size<X: Ord + Clone>(s: &std_Set<X>) -> u64 {
    s.x.len() as u64
}

pub fn std_set_empty<X: Ord + Clone>() -> std_Set<X> {
    std_Set::new()
}

pub fn std_set_singleton<X: Ord + Clone>(v: &X) -> std_Set<X> {
    let mut s = std_Set::new();
    s.insert(v.clone());
    s
}

pub fn std_set_insert<X: Ord+Clone>(s: &mut std_Set<X>, v: &X) {
    s.x.insert((*v).clone());
}

pub fn std_set_insert_imm<X: Ord+Clone>(s: &std_Set<X>, v: &X) -> std_Set<X> {
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

pub fn std_set_nth<X: Ord + Clone>(s: &std_Set<X>, n: &u64) -> std_Option<X> {
    option2std(s.x.iter().nth(*n as usize).cloned())
}

pub fn std_set2vec<X: Ord + Clone>(s: &std_Set<X>) -> std_Vec<X> {
    std_Vec{x: s.x.iter().cloned().collect()}
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
    };
    std_Set{x: s}
}


// Map

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug, Default)]
pub struct std_Map<K: Ord,V> {
    pub x: BTreeMap<K,V>
}

impl <K: Ord, V> std_Map<K,V> {
    pub fn new() -> Self {
        std_Map{x: BTreeMap::new()}
    }
    pub fn insert(&mut self, k: K, v: V) {
        self.x.insert(k,v);
    }
}

impl<K: FromRecord+Ord, V: FromRecord> FromRecord for std_Map<K,V> {
    fn from_record(val: &Record) -> Result<Self, String> {
        BTreeMap::from_record(val).map(|x|std_Map{x})
    }
}

impl<K: IntoRecord + Ord, V: IntoRecord> IntoRecord for std_Map<K,V> {
    fn into_record(self) -> Record {
        self.x.into_record()
    }
}

impl<K: FromRecord + Ord, V: FromRecord + PartialEq> Mutator<std_Map<K,V>> for Record
{
    fn mutate(&self, map: &mut std_Map<K,V>) -> Result<(), String> {
        self.mutate(&mut map.x)
    }
}

impl<K: Ord,V> IntoIterator for std_Map<K,V> {
    type Item = (K,V);
    type IntoIter = btree_map::IntoIter<K,V>;
    fn into_iter(self) -> Self::IntoIter {
        self.x.into_iter()
    }
}

impl<K: Ord, V> FromIterator<(K,V)> for std_Map<K,V> {
    fn from_iter<I>(iter: I) -> Self
    where I: IntoIterator<Item = (K,V)>
    {
        std_Map{x: BTreeMap::from_iter(iter)}
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

pub fn std_map_empty<K: Ord + Clone,V: Clone>() -> std_Map<K, V> {
    std_Map::new()
}

pub fn std_map_singleton<K: Ord + Clone,V: Clone>(k: &K, v: &V) -> std_Map<K, V> {
    let mut m = std_Map::new();
    m.insert(k.clone(), v.clone());
    m
}

pub fn std_map_insert<K: Ord+Clone, V: Clone>(m: &mut std_Map<K,V>, k: &K, v: &V) {
    m.x.insert((*k).clone(), (*v).clone());
}

pub fn std_map_remove<K: Ord+Clone, V: Clone>(m: &mut std_Map<K,V>, k: &K) {
    m.x.remove(k);
}

pub fn std_map_insert_imm<K: Ord+Clone, V: Clone>(m: &std_Map<K,V>, k: &K, v: &V) -> std_Map<K,V> {
    let mut m2 = m.clone();
    m2.insert((*k).clone(), (*v).clone());
    m2
}



pub fn std_map_get<K: Ord, V: Clone>(m: &std_Map<K,V>, k: &K) -> std_Option<V> {
    option2std(m.x.get(k).cloned())
}

pub fn std_map_contains_key<K: Ord, V: Clone>(s: &std_Map<K,V>, k: &K) -> bool {
    s.x.contains_key(k)
}

pub fn std_map_is_empty<K: Ord, V: Clone>(m: &std_Map<K,V>) -> bool {
    m.x.is_empty()
}



pub fn std_map_union<K: Ord + Clone,V: Clone>(m1: &std_Map<K,V>, m2: &std_Map<K,V>) -> std_Map<K, V> {
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

pub fn std_string_join(strings: &std_Vec<String>, sep: &String) -> String {
    strings.x.join(sep.as_str())
}

pub fn std_string_split(s: &String, sep: &String) -> std_Vec<String> {
    std_Vec{x: s.split(sep).map(|x| x.to_owned()).collect()}
}

pub fn std_string_contains(s1: &String, s2: &String) -> bool {
    s1.contains(s2.as_str())
}

pub fn std_string_substr(s: &String, start: &u64, end: &u64) -> String {
    let len = s.len();
    let from = cmp::min(*start as usize, len-1);
    let to = cmp::max(from, cmp::min(*end as usize, len));
    s[from..to].to_string()
}

pub fn std_str_to_lower(s: &String) -> String {
    s.to_lowercase()
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

pub fn std_group2set<A: Ord, G: Group<A>+?Sized>(g: &G) -> std_Set<A> {
    let mut res = std_Set::new();
    for i in 0..g.size() {
        res.insert(g.ith(i));
    };
    res
}

pub fn std_group_set_unions<A: Ord+Clone, G: Group<std_Set<A>>+?Sized>(g: &G) -> std_Set<A> {
    let mut res = std_Set::new();
    for i in 0..g.size() {
        res.x.append(&mut g.ith(i).x.clone());
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

pub fn std_group2map<K: Ord, V, G:Group<(K,V)>+?Sized>(g: &G) -> std_Map<K,V> {
    let mut res = std_Map::new();
    for i in 0..g.size() {
        let (k,v) = g.ith(i);
        res.insert(k, v);
    };
    res
}

pub fn std_group_unzip<X: Ord, Y: Ord, G: Group<(X,Y)>+?Sized>(g: &G) -> (std_Vec<X>, std_Vec<Y>) {
    let mut xs = std_Vec::new();
    let mut ys = std_Vec::new();
    for i in 0..g.size() {
        let (x,y) = g.ith(i);
        xs.push(x);
        ys.push(y);
    };
    (xs,ys)
}

pub fn std_group_first<A: Ord, G: Group<A>+?Sized>(g: &G) -> A {
    g.ith(0)
}

/* Tuples */
#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct tuple0;

impl FromRecord for tuple0 {
    fn from_record(val: &Record) -> Result<Self, String> {
        <()>::from_record(val).map(|_|tuple0)
    }
}

impl IntoRecord for tuple0 {
    fn into_record(self) -> Record {
        ().into_record()
    }
}


macro_rules! decl_tuple {
    ( $name:ident, $( $t:tt ),+ ) => {
        #[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
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

decl_tuple!(tuple2,  T1, T2);
decl_tuple!(tuple3,  T1, T2, T3);
decl_tuple!(tuple4,  T1, T2, T3, T4);
decl_tuple!(tuple5,  T1, T2, T3, T4, T5);
decl_tuple!(tuple6,  T1, T2, T3, T4, T5, T6);
decl_tuple!(tuple7,  T1, T2, T3, T4, T5, T6, T7);
decl_tuple!(tuple8,  T1, T2, T3, T4, T5, T6, T7, T8);
decl_tuple!(tuple9,  T1, T2, T3, T4, T5, T6, T7, T8, T9);
decl_tuple!(tuple10, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
decl_tuple!(tuple11, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
decl_tuple!(tuple12, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
decl_tuple!(tuple13, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
decl_tuple!(tuple14, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
decl_tuple!(tuple15, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
decl_tuple!(tuple16, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
decl_tuple!(tuple17, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17);
decl_tuple!(tuple18, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18);
decl_tuple!(tuple19, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19);
decl_tuple!(tuple20, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20);

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
