extern crate datalog_example;
extern crate differential_datalog;

use std::sync::Arc;
use std::process::exit;

use datalog_example::*;
use differential_datalog::*;

pub type UpdateCallback<V> = Arc<Fn(&V, bool) + Send + Sync>;

fn upd_cb(v: &Value, pol: bool) {
    println!("{} {:?}", if pol { "insert" } else { "delete" }, *v);
}

pub fn main() {
    let ret = run_interactive(Arc::new(|v,pol| upd_cb(v,pol)));
    exit(ret);
}
