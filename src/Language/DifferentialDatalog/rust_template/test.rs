#![allow(non_camel_case_types)]

#[cfg(test)]

use program::*;
use uint::*;
use abomonation::Abomonation;

use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdValSpine;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
enum Value {
    bool(bool),
    Uint(Uint),
    String(String),
    u8(u8),
    u16(u16),
    u32(u32),
    u64(u64)
}
unsafe_abomonate!(Value);

#[test]
fn test_simple_prog() {
    let prog: Program<Value> = Program {
        nodes: Vec::new()
    };
    let running: RunningProgram<Value, TraceAgent<Value, Value, Product<RootTimestamp,u64>, isize, OrdValSpine<Value, Value, Product<RootTimestamp,u64>, isize>>> = prog.run();
}
