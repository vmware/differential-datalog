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
pub struct tinyset_Set64<T: u64set::Fits64> {
    pub x: u64set::Set64<T>
}

/* This is needed so we can support for-loops over `Set64`'s.
 *
 * IMPORTANT: We iterate over Set64 by converting it into a sorted vector.
 * This can be costly, but is necessary to make sure that all `Set64`
 * operations are deterministic.
 */
pub struct Set64Iter<X:u64set::Fits64 + Ord> {
    iter: vec::IntoIter<X>
}

impl<X: u64set::Fits64 + Ord> Set64Iter<X> {
    pub fn new(set: &tinyset_Set64<X>) -> Set64Iter<X> {
        let mut v: Vec<_> = set.x.iter().collect();
        v.sort();
        Set64Iter{iter: v.into_iter()}
    }
}

impl<X:u64set::Fits64 + Ord> Iterator for Set64Iter<X> {
    type Item = X;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl <T: tinyset::Fits64 + Ord> tinyset_Set64<T> {
    pub fn iter(&self) -> Set64Iter<T> {
        Set64Iter::new(self)
    }
}

impl <T: tinyset::Fits64> tinyset_Set64<T> {
    /* In cases when order really, definitely, 100% does not matter,
     * a more efficient iterator can be used.
     */
    pub fn unsorted_iter(&self) -> u64set::Iter64<T> {
        self.x.iter()
    }
}

impl<T: fmt::Debug + u64set::Fits64 + Ord> fmt::Debug for tinyset_Set64<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        let len = self.x.len();
        for (i, x) in self.iter().enumerate() {
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
        let size_cmp = self.x.len().cmp(&other.x.len());
        if size_cmp != cmp::Ordering::Equal {
            size_cmp
        } else {
            self.iter().cmp(other.iter())
        }
    }
}

impl<T: u64set::Fits64 + Ord> PartialOrd for tinyset_Set64<T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
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
        serializer.collect_seq(self.iter())
    }
}

impl<'de, T: u64set::Fits64 + serde::Deserialize<'de> + Ord> serde::Deserialize<'de> for tinyset_Set64<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de>
    {
        panic!("tinyset_Set64::deserialize is not implemented")
    }
}

impl <T: u64set::Fits64> tinyset_Set64<T> {
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

impl<T: FromRecord + u64set::Fits64> FromRecord for tinyset_Set64<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        vec::Vec::from_record(val).map(|v|tinyset_Set64{x: u64set::Set64::from_iter(v)})
    }
}

impl<T: IntoRecord + u64set::Fits64 + Ord> IntoRecord for tinyset_Set64<T> {
    fn into_record(self) -> Record {
        Record::Array(CollectionKind::Set, self.into_iter().map(|x|x.into_record()).collect())
    }
}

impl<T: FromRecord + u64set::Fits64 + Ord> Mutator<tinyset_Set64<T>> for Record
{
    fn mutate(&self, set: &mut tinyset_Set64<T>) -> Result<(), String> {
        let upd = <tinyset_Set64<T>>::from_record(self)?;
        for v in upd.into_iter() {
            if !set.remove(&v) {
                set.insert(v);
            }
        };
        Ok(())
    }
}

impl<T: u64set::Fits64 + Ord> iter::IntoIterator for &tinyset_Set64<T> {
    type Item = T;
    type IntoIter = Set64Iter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: u64set::Fits64> FromIterator<T> for tinyset_Set64<T> {
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
        for (i, v) in self.iter().enumerate() {
            formatter.write_fmt(format_args!("{}", v))?;
            if i < len-1 {
                formatter.write_str(",")?;
            }
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

pub fn tinyset_size<X: u64set::Fits64>(s: &tinyset_Set64<X>) -> u64 {
    s.x.len() as u64
}

pub fn tinyset_empty<X: u64set::Fits64>() -> tinyset_Set64<X> {
    tinyset_Set64::new()
}

pub fn tinyset_singleton<X: u64set::Fits64 + Clone>(v: &X) -> tinyset_Set64<X> {
    let mut s = tinyset_Set64::new();
    s.insert(v.clone());
    s
}

pub fn tinyset_insert<X: u64set::Fits64 + Clone>(s: &mut tinyset_Set64<X>, v: &X) {
    s.x.insert((*v).clone());
}

pub fn tinyset_insert_imm<X: u64set::Fits64 + Clone>(s: &tinyset_Set64<X>, v: &X) -> tinyset_Set64<X> {
    let mut s2 = s.clone();
    s2.insert((*v).clone());
    s2
}

pub fn tinyset_contains<X: u64set::Fits64>(s: &tinyset_Set64<X>, v: &X) -> bool {
    s.x.contains(*v)
}

pub fn tinyset_is_empty<X: u64set::Fits64>(s: &tinyset_Set64<X>) -> bool {
    s.x.len() == 0
}

pub fn tinyset_nth<X: u64set::Fits64 + Ord + Clone>(s: &tinyset_Set64<X>, n: &u64) -> std_Option<X> {
    option2std(s.iter().nth(*n as usize))
}

pub fn tinyset_set2vec<X: u64set::Fits64 + Ord + Clone>(s: &tinyset_Set64<X>) -> std_Vec<X> {
    let mut v: Vec<_> = s.x.iter().collect();
    v.sort();
    std_Vec{x: v}
}

pub fn tinyset_union<X: u64set::Fits64 + Clone>(s1: &tinyset_Set64<X>, s2: &tinyset_Set64<X>) -> tinyset_Set64<X> {
    let s = s1.x.clone();
    tinyset_Set64{x: s.bitor(&s2.x)}
}

pub fn tinyset_unions<X: u64set::Fits64 + Clone>(sets: &std_Vec<tinyset_Set64<X>>) -> tinyset_Set64<X> {
    let mut s = u64set::Set64::new();
    for si in sets.x.iter() {
        for v in si.unsorted_iter() {
            s.insert(v);
        }

    };
    tinyset_Set64{x: s}
}

pub fn tinyset_group2set<X: u64set::Fits64>(g: &std_Group<X>) -> tinyset_Set64<X> {
    let mut res = tinyset_Set64::new();
    for ref v in g.iter() {
        tinyset_insert(&mut res, v);
    };
    res
}

pub fn tinyset_group_set_unions<X: u64set::Fits64 + Clone>(g: &std_Group<tinyset_Set64<X>>) -> tinyset_Set64<X>
{
    let mut res = u64set::Set64::new();
    for gr in g.iter() {
        for v in gr.unsorted_iter() {
            res.insert(v.clone());
        }
        //`extend` should be more efficient, but is not yet implemented in the tinyset library
        //res.extend(&mut g.ith(i).x.iter());
    };
    tinyset_Set64{x: res}
}

pub fn tinyset_group_setref_unions<X: u64set::Fits64 + Ord + Clone>(g: &std_Group<std_Ref<tinyset_Set64<X>>>)
    -> std_Ref<tinyset_Set64<X>> 
{
    if std_group_count(g) == 1 {
        std_group_first(g)
    } else {
        let mut res = std_ref_new(&tinyset_Set64{x: u64set::Set64::new()});
        {
            let rres = std_Ref::get_mut(&mut res).unwrap();
            for gr in g.iter() {
                for v in gr.unsorted_iter() {
                    rres.insert(v.clone());
                }
                //not implemented
                //res.extend(&mut g.ith(i).x.iter());
            };
        }
        res
    }
}
