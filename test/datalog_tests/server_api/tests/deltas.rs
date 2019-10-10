use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use differential_datalog::record::{Record, RelIdentifier, UpdCmd};
use distributed_datalog::Observable;
use distributed_datalog::Observer;
use distributed_datalog::SharedObserver;
use distributed_datalog::TcpReceiver;
use distributed_datalog::TcpSender;

use server_api_ddlog::api::*;
use server_api_ddlog::server::*;
use server_api_ddlog::Relations::*;

use maplit::hashmap;
use maplit::hashset;

use test_env_log::test;

use waitfor::wait_for;

fn single_delta_test<F>(setup: F) -> Result<(), String>
where
    F: FnOnce(&mut UpdatesObservable, SharedObserver<DDlogServer>) -> Result<Box<dyn Any>, String>,
{
    let program1 = HDDlog::run(1, false, |_, _: &Record, _| {});
    let mut server1 = DDlogServer::new(program1, hashmap! {});

    let deltas = Arc::new(Mutex::new(Vec::new()));
    let deltas2 = deltas.clone();
    let program2 = HDDlog::run(1, false, move |relation_id, record: &Record, _| {
        deltas2.lock().unwrap().push((relation_id, record.clone()));
    });
    let server2 = DDlogServer::new(program2, hashmap! {P1Out => P2In});

    let mut stream = server1.add_stream(hashset! {P1Out});
    let _data = setup(&mut stream, SharedObserver::new(server2))?;

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
        let deltas = deltas.lock().unwrap();
        if deltas.is_empty() {
            Ok(None)
        } else {
            Ok(Some(deltas.clone()))
        }
    });

    let expected = vec![(P2Out as usize, Record::String("delta-me-now".to_string()))];
    assert_eq!(result, Ok(Some(expected)));
    Ok(())
}

/// Verify that deltas from one program are reported properly in another
/// that is observing.
#[test]
fn single_delta_direct() -> Result<(), String> {
    fn do_test(
        observable: &mut UpdatesObservable,
        observer: SharedObserver<DDlogServer>,
    ) -> Result<Box<dyn Any>, String> {
        let _ = observable.subscribe(Box::new(observer)).unwrap();
        Ok(Box::new(()))
    }

    single_delta_test(do_test)
}

/// Verify that deltas from one program are reported properly in another
/// that is observing, using a TCP channel to transfer them over.
#[test]
fn single_delta_tcp() -> Result<(), String> {
    fn do_test(
        observable: &mut UpdatesObservable,
        observer: SharedObserver<DDlogServer>,
    ) -> Result<Box<dyn Any>, String> {
        let mut recv = TcpReceiver::new("127.0.0.1:0").unwrap();
        let send = TcpSender::connect(*recv.addr()).unwrap();

        let _ = recv.subscribe(Box::new(observer)).unwrap();
        let _ = observable.subscribe(Box::new(send)).unwrap();

        // We need to pass our receiver out so that it doesn't get
        // dropped immediately, which would result in a broken
        // connection.
        Ok(Box::new(recv))
    }

    single_delta_test(do_test)
}
