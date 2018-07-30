#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals)]

extern crate fnv;

#[macro_use] 
extern crate serde_derive;
extern crate serde;

extern crate differential_datalog;
extern crate cmd_parser;

#[macro_use] 
extern crate abomonation;

use differential_datalog::program::*;
use differential_datalog::uint::*;
use differential_datalog::int::*;
use cmd_parser::*;
use abomonation::Abomonation;

use std::sync::{Arc,Mutex};
use fnv::FnvHashMap;
use fnv::FnvHashSet;
use std::iter::FromIterator;
use std::fmt::Display;
use std::process::exit;

use serde::de::*;
use serde::ser::*;

fn __builtin_2string<T: Display>(x: &T) -> String {
    format!("{}", *x).to_string()
}

pub type ValMap = FnvHashMap<RelId, FnvHashSet<Value>>;

fn set_update(relid: RelId, s: &Arc<Mutex<ValMap>>, x : &Value, insert: bool)
{
    //println!("set_update({}) {:?} {}", rel, *x, insert);
    if insert {
        s.lock().unwrap().entry(relid).or_insert(FnvHashSet::default()).insert(x.clone());
    } else {
        s.lock().unwrap().entry(relid).or_insert(FnvHashSet::default()).remove(x);
    }
}
