extern crate tinyset;

use self::tinyset::u64set;
use std::vec;
use std::iter;
use std::cmp;
use std::fmt;
use serde;
use differential_datalog::record::*;
use std::iter::FromIterator;
use __std::option2std;
use std::ops::BitOr;
use std::collections;

#[derive(Eq, Clone, Hash, PartialEq)]
pub struct tinyset_Set64<T: u64set::Fits64 + Ord> {
    pub x: u64set::Set64<T>
}

impl<T: fmt::Debug + u64set::Fits64 + Ord> fmt::Debug for tinyset_Set64<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let sorted: collections::BTreeSet<T> = self.x.iter().collect();
        write!(f, "[")?;
        let len = sorted.len();
        for (i, x) in sorted.iter().enumerate() {
            if i == len - 1 {
                write!(f, "{:?}", x)?;
            } else {
                write!(f, "{:?}, ", x)?;
            }
        };
        write!(f, "]")
    }
}

impl<T: u64set::Fits64 + Ord> Ord for tinyset_Set64<T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.x.iter().cmp(other.x.iter())
    }
}

impl<T: u64set::Fits64 + Ord> PartialOrd for tinyset_Set64<T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.x.iter().partial_cmp(other.x.iter())
    }
}

impl<T: u64set::Fits64 + Ord> Default for tinyset_Set64<T> {
    fn default() -> Self {
        tinyset_Set64{x: u64set::Set64::default()}
    }
}

impl<T: u64set::Fits64 + serde::Serialize + Ord> serde::Serialize for tinyset_Set64<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer
    {
        serializer.collect_seq(self.x.iter())
    }
}

impl<'de, T: u64set::Fits64 + serde::Deserialize<'de> + Ord> serde::Deserialize<'de> for tinyset_Set64<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de>
    {
        panic!("tinyset_Set64::deserialize is not implemented")
    }
}

impl <T: u64set::Fits64 + Ord> tinyset_Set64<T> {
    pub fn new() -> Self {
        tinyset_Set64{x: u64set::Set64::new()}
    }
    pub fn insert(&mut self, v: T) {
        self.x.insert(v);
    }
    pub fn remove(&mut self, v: &T) -> bool {
        self.x.remove(v)
    }
}

impl<T: FromRecord + u64set::Fits64 + Ord> FromRecord for tinyset_Set64<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        vec::Vec::from_record(val).map(|v|tinyset_Set64{x: u64set::Set64::from_iter(v)})
    }
}

impl<T: IntoRecord + u64set::Fits64 + Ord> IntoRecord for tinyset_Set64<T> {
    fn into_record(self) -> Record {
        Record::Array(CollectionKind::Set, self.x.into_iter().map(|x|x.into_record()).collect())
    }
}

impl<T: FromRecord + u64set::Fits64 + Ord> Mutator<tinyset_Set64<T>> for Record
{
    fn mutate(&self, set: &mut tinyset_Set64<T>) -> Result<(), String> {
        let upd = <tinyset_Set64<T>>::from_record(self)?;
        for v in upd.x.into_iter() {
            if !set.remove(&v) {
                set.insert(v);
            }
        };
        Ok(())
    }
}

impl<'a, T: u64set::Fits64 + Ord> iter::IntoIterator for &'a tinyset_Set64<T> {
    type Item = T;
    type IntoIter = vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        let v: Vec<T> = self.x.iter().collect();
        v.into_iter()
    }
}

impl<T: u64set::Fits64 + Ord> FromIterator<T> for tinyset_Set64<T> {
    fn from_iter<I>(iter: I) -> Self
    where I: iter::IntoIterator<Item = T>
    {
        tinyset_Set64{x: u64set::Set64::from_iter(iter)}
    }
}


impl<T: Display + u64set::Fits64 + Ord> Display for tinyset_Set64<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let len = self.x.len();
        formatter.write_str("[")?;
        for (i, v) in self.x.iter().enumerate() {
            formatter.write_fmt(format_args!("{}", v))?;
            if i < len-1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

pub fn tinyset_size<X: u64set::Fits64+ Ord + Clone>(s: &tinyset_Set64<X>) -> u64 {
    s.x.len() as u64
}

pub fn tinyset_empty<X: u64set::Fits64 + Ord + Clone>() -> tinyset_Set64<X> {
    tinyset_Set64::new()
}

pub fn tinyset_singleton<X: u64set::Fits64 + Ord + Clone>(v: &X) -> tinyset_Set64<X> {
    let mut s = tinyset_Set64::new();
    s.insert(v.clone());
    s
}

pub fn tinyset_insert<X: u64set::Fits64 + Ord + Clone>(s: &mut tinyset_Set64<X>, v: &X) {
    s.x.insert((*v).clone());
}

pub fn tinyset_insert_imm<X: u64set::Fits64 + Ord + Clone>(s: &tinyset_Set64<X>, v: &X) -> tinyset_Set64<X> {
    let mut s2 = s.clone();
    s2.insert((*v).clone());
    s2
}

pub fn tinyset_contains<X: u64set::Fits64 + Ord>(s: &tinyset_Set64<X>, v: &X) -> bool {
    s.x.contains(*v)
}

pub fn tinyset_is_empty<X: u64set::Fits64 + Ord>(s: &tinyset_Set64<X>) -> bool {
    s.x.len() == 0
}

pub fn tinyset_nth<X: u64set::Fits64 + Ord + Clone>(s: &tinyset_Set64<X>, n: &u64) -> std_Option<X> {
    option2std(s.x.iter().nth(*n as usize))
}

pub fn tinyset_set2vec<X: u64set::Fits64 + Ord + Clone>(s: &tinyset_Set64<X>) -> std_Vec<X> {
    std_Vec{x: s.x.iter().collect()}
}

pub fn tinyset_union<X: u64set::Fits64 + Ord + Clone>(s1: &tinyset_Set64<X>, s2: &tinyset_Set64<X>) -> tinyset_Set64<X> {
    let s = s1.x.clone();
    tinyset_Set64{x: s.bitor(&s2.x)}
}

pub fn std_set_unions<X: u64set::Fits64 + Ord + Clone>(sets: &std_Vec<tinyset_Set64<X>>) -> tinyset_Set64<X> {
    let mut s = u64set::Set64::new();
    for si in sets.x.iter() {
        s.extend(si.x.iter());
    };
    tinyset_Set64{x: s}
}

pub fn tinyset_group2set<X: u64set::Fits64 + Ord, G: Group<X>+?Sized>(g: &G) -> tinyset_Set64<X> {
    let mut res = tinyset_Set64::new();
    for i in 0..g.size() {
        res.insert(g.ith(i));
    };
    res
}

pub fn tinyset_group_set_unions<X: u64set::Fits64 + Ord + Clone, G: Group<tinyset_Set64<X>>+?Sized>(g: &G) -> tinyset_Set64<X> {
    let mut res = u64set::Set64::new();
    for i in 0..g.size() {
        for v in g.ith(i).x.iter() {
            res.insert(v);
        }
        //`extend` should be more efficient, but is not yet implemented in the tinyset library
        //res.extend(&mut g.ith(i).x.iter());
    };
    tinyset_Set64{x: res}
}

pub fn tinyset_group_setref_unions<X: u64set::Fits64 + Ord + Clone, G: Group<std_Ref<tinyset_Set64<X>>>+?Sized>(g: &G)
    -> std_Ref<tinyset_Set64<X>> 
{
    if g.size() == 1 {
        g.ith(0)
    } else {
        let mut res = std_ref_new(&tinyset_Set64{x: u64set::Set64::new()});
        {
            let rres = std_Ref::get_mut(&mut res).unwrap();
            for i in 0..g.size() {
                for v in g.ith(i).x.iter() {
                    rres.insert(v);
                }
                //not implemented
                //res.extend(&mut g.ith(i).x.iter());
            };
        }
        res
    }
}
