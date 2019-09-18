#![allow(
    clippy::for_kv_map,
    clippy::get_unwrap,
    clippy::into_iter_on_ref,
    clippy::let_and_return,
    clippy::needless_return,
    clippy::new_without_default,
    clippy::ptr_arg,
    clippy::ptr_offset_with_cast,
    clippy::should_implement_trait,
    clippy::type_complexity
)]

#[macro_use]
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
