//! Tracks the state of DDlog output relations in memory.  Provides update callback to be invoked when the DB
//! changes; implements methods to dump the entire database or individual relations.
//! Used for testing.

#![allow(non_snake_case, dead_code)]

use std::io;
use std::convert::{AsMut, AsRef};
use std::collections::btree_map::{BTreeMap, Entry};
use std::collections::BTreeSet;

use super::*;

/* Stores the snapshot of output tables.
 */
#[derive(Default)]
pub struct ValMap{map: BTreeMap<RelId, BTreeSet<Value>>}

impl ValMap {
    pub fn new() -> ValMap {
        ValMap{map: BTreeMap::default()}
    }

    pub fn format(&self, w: &mut io::Write) -> io::Result<()> {
       for (relid, relset) in &self.map {
           w.write_fmt(format_args!("{:?}:\n", relid2rel(*relid).unwrap()))?;
           for val in relset {
                w.write_fmt(format_args!("{}\n", *val))?;
           };
           w.write_fmt(format_args!("\n"))?;
       };
       Ok(())
    }

    pub fn format_rel(&mut self, relid: RelId, w: &mut io::Write) -> io::Result<()> {
        let set = self.get_rel(relid);
        for val in set {
            w.write_fmt(format_args!("{}\n", *val))?;
        };
        Ok(())
    }

    pub fn get_rel(&mut self, relid: RelId) -> &BTreeSet<Value> {
        self.map.entry(relid).or_insert_with(BTreeSet::default)
    }

    pub fn update(&mut self, relid: RelId, x : &Value, insert: bool)
    {
        //println!("set_update({}) {:?} {}", rel, *x, insert);
        if insert {
            self.map.entry(relid).or_insert_with(BTreeSet::default).insert(x.clone());
        } else {
            self.map.entry(relid).or_insert_with(BTreeSet::default).remove(x);
        }
    }
}

/* Stores a set of changes to output tables.
 */
#[derive(Default)]
pub struct DeltaMap{map: BTreeMap<RelId, BTreeMap<Value, isize>>}

impl AsMut<BTreeMap<RelId, BTreeMap<Value, isize>>> for DeltaMap {
    fn as_mut(&mut self) -> &mut BTreeMap<RelId, BTreeMap<Value, isize>> {
        &mut self.map
    }
}

impl AsRef<BTreeMap<RelId, BTreeMap<Value, isize>>> for DeltaMap {
    fn as_ref(&self) -> &BTreeMap<RelId, BTreeMap<Value, isize>> {
        &self.map
    }
}

impl DeltaMap {
    pub fn new() -> DeltaMap {
        DeltaMap{map: BTreeMap::default()}
    }

    pub fn format(&self, w: &mut io::Write) -> io::Result<()> {
       for (relid, relmap) in &self.map {
           w.write_fmt(format_args!("{:?}:\n", relid2rel(*relid).unwrap()))?;
           for (val, weight) in relmap {
                w.write_fmt(format_args!("{}: {}\n", *val, *weight))?;
           };
           w.write_fmt(format_args!("\n"))?;
       };
       Ok(())
    }

    pub fn format_rel(&mut self, relid: RelId, w: &mut io::Write) -> io::Result<()> {
        let map = self.get_rel(relid);
        for (val, weight) in map {
            w.write_fmt(format_args!("{}: {}\n", *val, *weight))?;
        };
        Ok(())
    }

    pub fn get_rel(&mut self, relid: RelId) -> &BTreeMap<Value, isize> {
        self.map.entry(relid).or_insert_with(BTreeMap::default)
    }

    pub fn update(&mut self, relid: RelId, x : &Value, insert: bool)
    {
        let diff = if insert { 1 } else { -1 };
        //println!("set_update({}) {:?} {}", rel, *x, insert);
        let entry = self.map.entry(relid)
            .or_insert_with(BTreeMap::default)
            .entry(x.clone());
        match entry {
            Entry::Vacant(vacant) => { vacant.insert(diff); },
            Entry::Occupied(mut occupied) => {
                if *occupied.get() == -diff {
                    occupied.remove();
                } else {
                    *occupied.get_mut() += diff;
                }
            }
        };
    }
}
