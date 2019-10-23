#![allow(clippy::get_unwrap, clippy::type_complexity)]

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
