use std::sync::Mutex;

use differential_datalog::record::{Record, RelIdentifier, UpdCmd};
use observe::Observable;
use observe::Observer;

use server_api_ddlog::api::*;
use server_api_ddlog::server;
use server_api_ddlog::Relations::*;

use maplit::hashmap;
use maplit::hashset;

/// Verify that deltas from one program are reported properly in another
/// that is observing.
#[test]
fn single_delta() -> Result<(), String> {
    // We need a global here (and cannot, say, close over some local
    // from a lambda expression) because our callback has to implement
    // Clone and that is not possible for a closure capturing its
    // environment in a mutable way.
    // We need an option because Vec::new is not const and so we can't
    // use it to initialize a static.
    static mut DELTAS: Option<Mutex<Vec<(usize, Record)>>> = None;
    unsafe { DELTAS = Some(Mutex::new(Vec::new())) };

    fn collect_deltas(relation_id: usize, record: &Record, _weight: isize) {
        let deltas = unsafe { DELTAS.as_mut().unwrap() };
        deltas.lock().unwrap().push((relation_id, record.clone()));
    }

    let program1 = HDDlog::run(1, false, |_, _: &Record, _| {});
    let mut server1 = server::DDlogServer::new(program1, hashmap! {});

    let program2 = HDDlog::run(1, false, collect_deltas);
    let server2 = server::DDlogServer::new(program2, hashmap! {P1Out => P2In});

    let mut stream = server1.add_stream(hashset! {P1Out});
    let _ = stream.subscribe(Box::new(server2));

    let updates = &[UpdCmd::Insert(
        RelIdentifier::RelId(P1In as usize),
        Record::String("delta-me-now".to_string()),
    )];

    server1.on_start()?;
    server1.on_updates(Box::new(
        updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
    ))?;
    server1.on_commit()?;

    let mutex = unsafe { DELTAS.as_mut().unwrap() };
    let deltas = mutex.lock().unwrap();
    let actual = deltas.as_slice();
    let expected = &[(P2Out as usize, Record::String("delta-me-now".to_string()))];
    assert_eq!(actual, expected);
    Ok(())
}
