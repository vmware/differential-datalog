#[macro_use]
extern crate abomonation;
extern crate num;

#[cfg(test)]
#[macro_use] 
extern crate serde_derive;
extern crate serde;

extern crate timely;
//extern crate timely_communication;
extern crate differential_dataflow;
extern crate fnv;

extern crate cmd_parser;

mod profile;
mod variable;
#[cfg(test)]
mod test;

pub mod program;
pub mod uint;
pub mod int;
pub mod arcval;
