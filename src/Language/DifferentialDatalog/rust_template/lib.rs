#[macro_use]
extern crate abomonation;
extern crate num;

#[macro_use] 
extern crate serde_derive;
extern crate serde;

extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;
extern crate fnv;

mod uint;
mod variable;
pub mod program;
mod test;
