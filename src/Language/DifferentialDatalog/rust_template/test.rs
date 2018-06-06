#![allow(non_camel_case_types)]

#[cfg(test)]

use program::*;
use uint::*;
use abomonation::Abomonation;

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
        relations: Vec::new(),
        nodes: Vec::new()
    };
    let running = prog.run();
}
