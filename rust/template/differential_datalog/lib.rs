#[macro_use]
extern crate abomonation;
extern crate num;

#[cfg(test)]
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate libc;

extern crate timely;
//extern crate timely_communication;
extern crate differential_dataflow;
extern crate fnv;
extern crate sequence_trie;

mod profile;
mod variable;
#[cfg(test)]
mod test;

pub mod program;
pub mod uint;
pub mod int;
pub mod arcval;

#[macro_use]
pub mod record;

#[cfg(test)]
mod test_record;
