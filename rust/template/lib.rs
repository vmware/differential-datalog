#![allow(non_camel_case_types)]

extern crate fnv;

#[macro_use] 
extern crate serde_derive;
extern crate serde;

extern crate differential_datalog;

#[macro_use] 
extern crate abomonation;

use differential_datalog::program::*;
use differential_datalog::uint::*;
use differential_datalog::int::*;
use abomonation::Abomonation;

use std::sync::{Arc,Mutex};
use fnv::FnvHashSet;
use std::iter::FromIterator;
