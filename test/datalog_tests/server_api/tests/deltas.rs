use std::sync::Mutex;

use differential_datalog::program::Update;
use differential_datalog::record::{Record, RelIdentifier, UpdCmd};
use observe::Observable;
use observe::Observer;
use observe::SharedObserver;

use server_api_ddlog::api::*;
use server_api_ddlog::Relations::*;
use server_api_ddlog::server::*;
use server_api_ddlog::Value;

use lazy_static::lazy_static;

use maplit::hashmap;
use maplit::hashset;

macro_rules! DeltaTest {
    ( $($setup_fn:tt)* ) => {{
        lazy_static! {
            static ref DELTAS: Mutex<Vec<(usize, Record)>> = {
                Mutex::new(Vec::new())
            };
        }

        fn collect_deltas(relation_id: usize, record: &Record, _weight: isize) {
            DELTAS.lock().unwrap().push((relation_id, record.clone()));
        }

        let program1 = HDDlog::run(1, false, |_, _: &Record, _| {});
        let mut server1 = DDlogServer::new(program1, hashmap! {});

        let program2 = HDDlog::run(1, false, collect_deltas);
        let server2 = DDlogServer::new(program2, hashmap! {P1Out => P2In});

        let mut stream = server1.add_stream(hashset! {P1Out});
        ($($setup_fn)*)(&mut stream, SharedObserver::new(server2))?;

        let updates = &[UpdCmd::Insert(
            RelIdentifier::RelId(P1In as usize),
            Record::String("delta-me-now".to_string()),
        )];

        server1.on_start()?;
        server1.on_updates(Box::new(
            updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
        ))?;
        server1.on_commit()?;

        let deltas = DELTAS.lock().unwrap();
        let actual = deltas.as_slice();
        let expected = &[(P2Out as usize, Record::String("delta-me-now".to_string()))];
        assert_eq!(actual, expected);
        Ok(())
    }}
}

/// Verify that deltas from one program are reported properly in another
/// that is observing.
#[test]
fn single_delta_direct() -> Result<(), String> {
    fn do_test(
        observable: &mut dyn Observable<Update<Value>, String>,
        observer: SharedObserver<DDlogServer>,
    ) -> Result<(), String> {
        observable.subscribe(Box::new(observer));
        Ok(())
    }

    DeltaTest!(do_test)
}
