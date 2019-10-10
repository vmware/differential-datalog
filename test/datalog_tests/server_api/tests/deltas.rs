use std::any::Any;
use std::sync::Mutex;
use std::time::Duration;

use differential_datalog::program::Update;
use differential_datalog::record::{Record, RelIdentifier, UpdCmd};
use observe::Observable;
use observe::Observer;
use observe::SharedObserver;
use tcp_channel::TcpReceiver;
use tcp_channel::TcpSender;

use server_api_ddlog::api::*;
use server_api_ddlog::server::*;
use server_api_ddlog::Relations::*;
use server_api_ddlog::Value;

use lazy_static::lazy_static;

use maplit::hashmap;
use maplit::hashset;

use test_env_log::test;

use waitfor::wait_for;

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
        let _data = ($($setup_fn)*)(&mut stream, SharedObserver::new(server2))?;

        let updates = &[UpdCmd::Insert(
            RelIdentifier::RelId(P1In as usize),
            Record::String("delta-me-now".to_string()),
        )];

        server1.on_start()?;
        server1.on_updates(Box::new(
            updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
        ))?;
        server1.on_commit()?;

        let result = wait_for::<_, _, ()>(Duration::from_secs(5), Duration::from_millis(10), || {
            let deltas = DELTAS.lock().unwrap();
            if deltas.is_empty() {
                Ok(None)
            } else {
                Ok(Some(deltas.clone()))
            }
        });

        let expected = vec![(P2Out as usize, Record::String("delta-me-now".to_string()))];
        assert_eq!(result, Ok(Some(expected)));
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
    ) -> Result<Box<dyn Any>, String> {
        observable.subscribe(Box::new(observer));
        Ok(Box::new(()))
    }

    DeltaTest!(do_test)
}

/// Verify that deltas from one program are reported properly in another
/// that is observing, using a TCP channel to transfer them over.
#[test]
fn single_delta_tcp() -> Result<(), String> {
    fn do_test(
        observable: &mut dyn Observable<Update<Value>, String>,
        observer: SharedObserver<DDlogServer>,
    ) -> Result<Box<dyn Any>, String> {
        let mut recv = TcpReceiver::new("127.0.0.1:0").unwrap();
        let send = TcpSender::connect(*recv.addr()).unwrap();

        recv.subscribe(Box::new(observer));
        observable.subscribe(Box::new(send));

        // We need to pass our receiver out so that it doesn't get
        // dropped immediately, which would result in a broken
        // connection.
        Ok(Box::new(recv))
    }

    DeltaTest!(do_test)
}
