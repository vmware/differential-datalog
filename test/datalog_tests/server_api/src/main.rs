use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use observe::{Observer, Observable};

use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;
use ddd_ddlog::server;

use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};

fn main() -> Result<(), String> {
    // Construct left server with no redirect
    let prog1 = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect1 = HashMap::new();
    redirect1.insert(lr_left_Left as usize, lr_left_Left as usize);
    let mut s1 = server::DDlogServer::new(prog1, redirect1);

    // Construct right server, redirect Middle table
    let prog2 = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect2 = HashMap::new();
    redirect2.insert(lr_left_Up as usize, lr_right_Up as usize);
    let s2 = server::DDlogServer::new(prog2, redirect2);

    // Stream table from left server
    let mut tables = HashSet::new();
    tables.insert(lr_left_Up as usize);
    let outlet = s1.add_stream(tables);

    // Right server subscribes to the stream
    let s2 = Arc::new(Mutex::new(s2));
    let sub = {
        let s2_a = server::ADDlogServer(s2.clone());
        let stream = outlet.clone();
        let mut stream = stream.lock().unwrap();
        stream.subscribe(Box::new(s2_a))
    };

    // Insert `true` to Left in left server
    let rec = Record::Bool(true);
    let table_id = RelIdentifier::RelId(lr_left_Left as usize);
    let updates = &[UpdCmd::Insert(table_id, rec)];

    // Execute and transmit the update
    s1.on_start()?;
    s1.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())))?;
    s1.on_commit()?;
    s1.on_completed()?;

    // Test unsubscribe
    sub.unsubscribe();

    let rec2 = Record::Bool(true);
    let table_id2 = RelIdentifier::RelId(lr_left_Left as usize);
    let updates2 = &[UpdCmd::Delete(table_id2, rec2)];

    s1.on_start()?;
    s1.on_updates(Box::new(updates2.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())))?;
    s1.on_commit()?;
    s1.on_completed()?;

    s1.remove_stream(outlet);
    s1.shutdown()?;
    s2.lock().unwrap().shutdown()?;
    Ok(())
}
