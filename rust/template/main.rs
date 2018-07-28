extern crate datalog_example;
extern crate differential_datalog;

use datalog_example::*;
use differential_datalog::*;

pub type UpdateCallback<V> = Arc<Fn(&V, bool) + Send + Sync>;

fn upd_cb(&Value, bool) {
}

pub fn main() {
    let ret = run_interactive(Arc::new(|v,pol| upd_cb(v,pol)));
    exit(ret);
}
