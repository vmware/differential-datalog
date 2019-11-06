//! Tracks the state of DDlog output relations in memory.  Provides update callback to be invoked when the DB
//! changes; implements methods to dump the entire database or individual relations.
//! Used for testing.

#![allow(non_snake_case, dead_code)]

use std::collections::btree_map::{BTreeMap, Entry};
use std::collections::BTreeSet;
use std::convert::{AsMut, AsRef};
use std::io;

use super::*;

/// Convert a `RelId` into something else.
pub trait ConvertRelId {
    /// Convert a `RelId` into its symbolic name.
    fn relid2name(relId: RelId) -> Option<&'static str>;
}

/* Stores a set of changes to output tables.
 */
#[derive(Debug, Default)]
pub struct DeltaMap<V> {
    map: BTreeMap<RelId, BTreeMap<V, isize>>,
}

impl<V> AsMut<BTreeMap<RelId, BTreeMap<V, isize>>> for DeltaMap<V> {
    fn as_mut(&mut self) -> &mut BTreeMap<RelId, BTreeMap<V, isize>> {
        &mut self.map
    }
}

impl<V> AsRef<BTreeMap<RelId, BTreeMap<V, isize>>> for DeltaMap<V> {
    fn as_ref(&self) -> &BTreeMap<RelId, BTreeMap<V, isize>> {
        &self.map
    }
}

impl<V: Val> DeltaMap<V> {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::default(),
        }
    }

    pub fn format<R>(&self, w: &mut dyn io::Write) -> io::Result<()>
    where
        R: ConvertRelId,
    {
        for (relid, relmap) in &self.map {
            w.write_fmt(format_args!("{}:\n", R::relid2name(*relid).unwrap()))?;
            for (val, weight) in relmap {
                w.write_fmt(format_args!("{}: {}\n", *val, *weight))?;
            }
            w.write_fmt(format_args!("\n"))?;
        }
        Ok(())
    }

    pub fn format_rel(&mut self, relid: RelId, w: &mut dyn io::Write) -> io::Result<()> {
        let map = self.get_rel(relid);
        for (val, weight) in map {
            w.write_fmt(format_args!("{}: {}\n", *val, *weight))?;
        }
        Ok(())
    }

    pub fn format_as_sets<R>(&self, w: &mut dyn io::Write) -> io::Result<()>
    where
        R: ConvertRelId,
    {
        for (relid, map) in &self.map {
            w.write_fmt(format_args!("{}:\n", R::relid2name(*relid).unwrap()))?;
            for (val, weight) in map {
                w.write_fmt(format_args!("{}\n", *val))?;
                assert_eq!(*weight, 1, "val={}, weight={}", *val, *weight);
            }
            w.write_fmt(format_args!("\n"))?;
        }
        Ok(())
    }

    pub fn format_rel_as_set(&mut self, relid: RelId, w: &mut dyn io::Write) -> io::Result<()> {
        let map = self.get_rel(relid);
        for (val, weight) in map {
            w.write_fmt(format_args!("{}\n", *val))?;
            assert_eq!(*weight, 1, "val={}, weight={}", *val, *weight);
        }
        Ok(())
    }

    pub fn get_rel(&mut self, relid: RelId) -> &BTreeMap<V, isize> {
        self.map.entry(relid).or_insert_with(BTreeMap::default)
    }

    pub fn update(&mut self, relid: RelId, x: &V, diff: isize) {
        //println!("set_update({}) {:?} {}", rel, *x, insert);
        let entry = self
            .map
            .entry(relid)
            .or_insert_with(BTreeMap::default)
            .entry(x.clone());
        match entry {
            Entry::Vacant(vacant) => {
                vacant.insert(diff);
            }
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
