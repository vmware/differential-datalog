//! Tracks the state of DDlog output relations in memory.  Provides update callback to be invoked when the DB
//! changes; implements methods to dump the entire database or individual relations.
//! Used for testing.

#![allow(non_snake_case, dead_code)]

use std::io;
use std::convert::{AsMut, AsRef};
use std::collections::btree_map::{BTreeMap, Entry};
use std::collections::BTreeSet;

use super::*;

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

    pub fn format_as_sets(&self, w: &mut io::Write) -> io::Result<()> {
       for (relid, map) in &self.map {
           w.write_fmt(format_args!("{:?}:\n", relid2rel(*relid).unwrap()))?;
           for (val,weight) in map {
                w.write_fmt(format_args!("{}\n", *val))?;
                assert_eq!(*weight, 1, "val={}, weight={}", *val, *weight);
           };
           w.write_fmt(format_args!("\n"))?;
       };
       Ok(())
    }

    pub fn format_rel_as_set(&mut self, relid: RelId, w: &mut io::Write) -> io::Result<()> {
        let map = self.get_rel(relid);
        for (val, weight) in map {
            w.write_fmt(format_args!("{}\n", *val))?;
            assert_eq!(*weight, 1, "val={}, weight={}", *val, *weight);
        };
        Ok(())
    }

    pub fn get_rel(&mut self, relid: RelId) -> &BTreeMap<Value, isize> {
        self.map.entry(relid).or_insert_with(BTreeMap::default)
    }

    pub fn update(&mut self, relid: RelId, x : &Value, diff: isize)
    {
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
