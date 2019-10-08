#![allow(clippy::get_unwrap, clippy::type_complexity)]

extern crate abomonation;
extern crate num;

#[cfg(test)]
#[macro_use]
extern crate serde_derive;
extern crate libc;
extern crate serde;

extern crate timely;
//extern crate timely_communication;
extern crate differential_dataflow;
extern crate fnv;
extern crate sequence_trie;

mod profile;
#[cfg(test)]
mod test;
mod variable;

pub mod arcval;
pub mod int;
pub mod program;
pub mod uint;

#[macro_use]
pub mod record;

#[cfg(test)]
mod test_record;
