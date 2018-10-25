//! Tracks the state of DDlog output relations in memory.  Provides update callback to be invoked when the DB
//! changes; implements methods to dump the entire database or individual relations.
//! Used for testing.

#![allow(non_snake_case, dead_code)]

use std::io;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::*;

pub struct ValMap{map: BTreeMap<RelId, BTreeSet<Value>>}

#[no_mangle]
pub fn val_map_new() -> *mut ValMap {
    Box::into_raw(Box::new(ValMap::new()))
}

#[no_mangle]
pub fn val_map_print(map: *mut ValMap) {
    let map = unsafe{&mut *map};
    map.format(&mut io::stdout());
}

#[no_mangle]
pub fn val_map_print_rel(map: *mut ValMap, relid: RelId) {
    let map = unsafe{&mut *map};
    map.format_rel(relid, &mut io::stdout());
}

#[no_mangle]
pub fn val_map_free(map: *mut ValMap) {
    unsafe{Box::from_raw(map);}
}

impl ValMap {
    pub fn new() -> ValMap {
        ValMap{map: BTreeMap::default()}
    }

    pub fn format(&self, w: &mut io::Write) {
       for (relid, relset) in &self.map {
           w.write_fmt(format_args!("{:?}:\n", relid2rel(*relid).unwrap()));
           for val in relset {
                w.write_fmt(format_args!("{}\n", *val));
           };
           w.write_fmt(format_args!("\n"));
       };
    }

    pub fn format_rel(&mut self, relid: RelId, w: &mut io::Write) {
        let set = self.get_rel(relid);
        for val in set {
            println!("{}", *val);
        };
    }

    pub fn get_rel(&mut self, relid: RelId) -> &BTreeSet<Value> {
        self.map.entry(relid).or_insert(BTreeSet::default())
    }

    pub fn update(&mut self, relid: RelId, x : &Value, insert: bool)
    {
        //println!("set_update({}) {:?} {}", rel, *x, insert);
        if insert {
            self.map.entry(relid).or_insert(BTreeSet::default()).insert(x.clone());
        } else {
            self.map.entry(relid).or_insert(BTreeSet::default()).remove(x);
        }
    }
}
