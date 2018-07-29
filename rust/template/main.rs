extern crate datalog_example;
extern crate differential_datalog;

use std::sync::Arc;
use std::process::exit;

use datalog_example::*;
use differential_datalog::*;
use differential_datalog::program::*;

fn upd_cb(relid: RelId, v: &Value, pol: bool) {
    println!("{} {:?} {:?}", if pol { "insert" } else { "delete" }, relid, *v);
}

pub fn main() {
    let ret = run_interactive(Arc::new(|relid,v,pol| upd_cb(relid,v,pol)));
    exit(ret);
}
