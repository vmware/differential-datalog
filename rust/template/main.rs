extern crate datalog_example;
extern crate differential_datalog;

use std::sync::Arc;
use std::sync::Mutex;
use std::process::exit;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use datalog_example::*;
use differential_datalog::program::*;


fn upd_cb(db: &Arc<Mutex<ValMap>>, relid: RelId, v: &Value, pol: bool) {
    set_update(relid, db, v, pol);
    eprintln!("{} {:?} {:?}", if pol { "insert" } else { "delete" }, relid, *v);
}

fn set_update(relid: RelId, s: &Arc<Mutex<ValMap>>, x : &Value, insert: bool)
{
    //println!("set_update({}) {:?} {}", rel, *x, insert);
    if insert {
        s.lock().unwrap().entry(relid).or_insert(BTreeSet::default()).insert(x.clone());
    } else {
        s.lock().unwrap().entry(relid).or_insert(BTreeSet::default()).remove(x);
    }
}


pub fn main() {
    let db: Arc<Mutex<ValMap>> = Arc::new(Mutex::new(BTreeMap::default()));

    let ret = run_interactive(db.clone(), Arc::new(move |relid,v,pol| upd_cb(&db,relid,v,pol)));
    exit(ret);
}
