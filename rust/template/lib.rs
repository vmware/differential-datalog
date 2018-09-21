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
use cmd_parser::*;
use abomonation::Abomonation;

use fnv::FnvHashSet;
use std::fmt::Display;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use twox_hash::XxHash;

pub mod ffi;
pub mod valmap;

const XX_SEED1: u64 = 0x23b691a751d0e108;
const XX_SEED2: u64 = 0x20b09801dce5ff84;

pub type std_Vec<T> = std::vec::Vec<T>;
pub type std_Set<T> = std::collections::HashSet<T>;

fn std___builtin_2string<T: Display>(x: &T) -> String {
    format!("{}", *x).to_string()
}

fn std_hex<T: fmt::LowerHex>(x: &T) -> String {
    format!("{:x}", *x).to_string()
}

fn std_hash64<T: Hash>(x: &T) -> u64 {
    let mut hasher = XxHash::with_seed(XX_SEED1);
    x.hash(&mut hasher);
    hasher.finish()
}

fn std_hash128<T: Hash>(x: &T) -> u128 {
    let mut hasher = XxHash::with_seed(XX_SEED1);
    x.hash(&mut hasher);
    let w1 = hasher.finish();
    let mut hasher = XxHash::with_seed(XX_SEED2);
    x.hash(&mut hasher);
    let w2 = hasher.finish();
    ((w1 as u128) << 64) | (w2 as u128)
}
