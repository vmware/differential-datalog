use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;
use ddd_ddlog::server;
use ddd_ddlog::channel::{Observable, Observer};

use std::collections::{HashSet, HashMap};

use differential_datalog::record::{Record, UpdCmd, RelIdentifier};

fn main() -> Result<(), String> {
    let prog1 = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect1 = HashMap::new();
    redirect1.insert(lr_left_Left as usize, lr_left_Left as usize);
    let mut s1 = server::DDlogServer::new(prog1, Vec::new(), redirect1);

    let prog2 = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect2 = HashMap::new();
    redirect2.insert(lr_left_Middle as usize, lr_right_Middle as usize);
    let s2 = server::DDlogServer::new(prog2, Vec::new(), redirect2);

    let mut tables = HashSet::new();
    tables.insert(lr_left_Middle as usize);
    let outlet = s1.stream(tables);

    outlet.subscribe(Box::new(s2));

    s1.on_start()?;

    let rec = Record::Bool(true);
    let table_id = RelIdentifier::RelId(lr_left_Left as usize);
    let updates = &[UpdCmd::Insert(table_id, rec)];
    s1.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())))?;
    s1.on_commit()?;
    s1.on_completed()
}
