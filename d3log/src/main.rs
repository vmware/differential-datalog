mod batch;
mod child;
mod json_framer;
mod tcp_network;
mod transact;

use crate::{batch::Batch, child::start_children, typedefs::matrix::Matrix};

use differential_datalog::ddval::DDValConvert;
use differential_datalog::D3logLocationId;
use differential_datalog::DeltaMap;
use mm_ddlog::*;
use rustop::opts;
use std::str;

type Node = D3logLocationId;

use std::convert::TryFrom;
fn matrix(mat: Vec<Vec<u64>>) -> Result<Batch, String> {
    let mut b = Batch::new(DeltaMap::new());
    let relid = Relations::try_from("matrix::Matrix")
        .map_err(|_| format!("Unknown relation {}", "Matrix"))?;

    for (j, c) in mat.iter().enumerate() {
        for (i, v) in c.iter().enumerate() {
            b.insert(
                relid as usize,
                Matrix {
                    i: i as u32,
                    j: j as u32,
                    v: (*v) as u32,
                }
                .into_ddvalue(),
                1,
            );
        }
    }
    Ok(b)
}

fn main() {
    let (args, _) = opts! {
        synopsis "D3log multiprocess test harness.";
        auto_shorts false;
        // --nodes or -n
        opt nodes:usize=1, short:'n', desc:"The number of worker processes. Default is 1.";
    }
    .parse_or_exit();

    // blocking
    start_children(
        args.nodes,
        match matrix(vec![vec![1, 2, 3], vec![7, 12, 19], vec![5, 3, 1]]) {
            Ok(x) => x,
            Err(x) => {
                println!("matrix construction error {}", x);
                return;
            }
        },
    )
}
