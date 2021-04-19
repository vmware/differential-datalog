mod json_framer;
mod batch;
mod tcp_network;
mod child;
mod transact;


use crate::{batch::Batch,
            child::start_children,
            typedefs::matrix::Matrix};

use differential_datalog::ddval::DDValConvert;
use std::str;
use rustop::opts;
use mm_ddlog::*;
use differential_datalog::DeltaMap;
use differential_datalog::{D3logLocationId};

type Node = D3logLocationId;

use std::convert::TryFrom;
fn matrix(mat:Vec::<Vec::<u64>>) -> Result<Batch, String> {
    let mut b = Batch::new(DeltaMap::new());
    let mut i = 0;
    let relid = Relations::try_from("matrix::Matrix").map_err(|_| format!("Unknown relation {}", "Matrix"))?;

    for c in mat.iter() {
        let mut j = 0;
        for v in c.iter() {
            b.insert(relid as usize, Matrix{i, j, v:(*v) as u32}.into_ddvalue(), 1);
            j = j+1;
        }
        i=i+1;
    }
    Ok(b)
}

fn main() {
    let (args, _) = opts! {
        synopsis "D3log multiprocess test harness.";
        auto_shorts false;
        // --nodes or -n        
        opt nodes:usize=1, short:'n', desc:"The number of worker processes. Default is 1.";
    }.parse_or_exit();


    // blocking
    start_children(args.nodes,
                   match matrix(vec![vec![1, 2, 3],
                                     vec![7, 12, 19],
                                     vec![5, 3, 1]]) {
                       Ok(x) => x, Err(x) => {
                           println!("matrix construction error {}", x);
                           return
                       }
                   })
}
