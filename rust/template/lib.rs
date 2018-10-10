#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals, unused_parens, non_shorthand_field_patterns, dead_code)]

extern crate fnv;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate libc;
extern crate twox_hash;

extern crate differential_datalog;
extern crate cmd_parser;

#[macro_use]
extern crate abomonation;

use differential_datalog::program::*;
use differential_datalog::uint::*;
use differential_datalog::int::*;
use differential_datalog::arcval;
use cmd_parser::*;
use abomonation::Abomonation;

use fnv::FnvHashSet;
use fnv::FnvHashMap;
use std::fmt::Display;
use std::fmt;
use std::sync;
use std::hash::Hash;
use std::hash::Hasher;

pub mod valmap;
mod stdlib;

use self::stdlib::*;
