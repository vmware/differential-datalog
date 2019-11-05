use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::spawn;
use std::time::Duration;

use differential_datalog::record::{Record, RelIdentifier, UpdCmd};
use distributed_datalog::Observable;
use distributed_datalog::Observer;
use distributed_datalog::SharedObserver;
use distributed_datalog::TcpReceiver;
use distributed_datalog::TcpSender;
use distributed_datalog::TxnMux;

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
    let program1 = HDDlog::run(1, false, |_, _: &Record, _| {}).unwrap();
    let mut server1 = DDlogServer::new(program1, hashmap! {});

    let deltas = Arc::new(Mutex::new(Vec::new()));
    let deltas2 = deltas.clone();
    let program2 = HDDlog::run(1, false, move |relation_id, record: &Record, _| {
        deltas2.lock().unwrap().push((relation_id, record.clone()));
    })
    .unwrap();
    let server2 = DDlogServer::new(
        program2,
        hashmap! {
            server_api_1_P1Out as usize => server_api_2_P2In as usize,
        },
    );

    let mut stream = server1.add_stream(hashset! {server_api_1_P1Out as usize});
    let _data = setup(&mut stream, SharedObserver::new(server2))?;

    let updates = &[UpdCmd::Insert(
        RelIdentifier::RelId(server_api_1_P1In as usize),
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

    let expected = vec![(
        server_api_2_P2Out as usize,
        Record::String("delta-me-now".to_string()),
    )];
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

fn multi_transaction_test<F>(setup: F) -> Result<(), String>
where
    F: FnOnce(UpdatesObservable, UpdatesObservable, DDlogServer) -> Result<Box<dyn Any>, String>,
{
    // The setup we build is as follows:
    //
    //       P3[s1 ++ s2]
    //         /      \
    //        /        \
    //     P1[s1]     P2[s2]
    //
    let program1 = HDDlog::run(1, false, |_, _: &Record, _| {}).unwrap();
    let mut server1 = DDlogServer::new(program1, hashmap! {});

    let program2 = HDDlog::run(1, false, |_, _: &Record, _| {}).unwrap();
    let mut server2 = DDlogServer::new(program2, hashmap! {});

    let deltas = Arc::new(Mutex::new(Vec::new()));
    let deltas2 = deltas.clone();
    let program3 = HDDlog::run(1, false, move |relation_id, record: &Record, _| {
        deltas2.lock().unwrap().push((relation_id, record.clone()));
    })
    .unwrap();
    let redirect = hashmap! {
        server_api_1_P1Out as usize => server_api_3_P1Out as usize,
        server_api_2_P2Out as usize => server_api_3_P2Out as usize,
    };
    let server3 = DDlogServer::new(program3, redirect);

    let stream1 = server1.add_stream(hashset! {server_api_1_P1Out as usize});
    let stream2 = server2.add_stream(hashset! {server_api_2_P2Out as usize});
    let _data = setup(stream1, stream2, server3)?;

    // Insert updates concurrently to test serialization of
    // transactions.
    let t1 = spawn(move || {
        let updates1 = &[UpdCmd::Insert(
            RelIdentifier::RelId(server_api_1_P1In as usize),
            Record::String("p1-entry".to_string()),
        )];

        server1.on_start().unwrap();
        server1
            .on_updates(Box::new(
                updates1.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
            ))
            .unwrap();
        server1.on_commit().unwrap();
    });

    let t2 = spawn(move || {
        let updates2 = &[UpdCmd::Insert(
            RelIdentifier::RelId(server_api_2_P2In as usize),
            Record::String("p2-entry".to_string()),
        )];

        server2.on_start().unwrap();
        server2
            .on_updates(Box::new(
                updates2.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
            ))
            .unwrap();
        server2.on_commit().unwrap();
    });

    let result = wait_for::<_, _, ()>(Duration::from_secs(5), Duration::from_millis(10), || {
        let deltas = deltas.lock().unwrap();
        if deltas.is_empty() {
            Ok(None)
        } else {
            Ok(Some(deltas.clone()))
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    let expected = vec![(
        server_api_3_P3Out as usize,
        Record::String("p1-entryp2-entry".to_string()),
    )];
    assert_eq!(result, Ok(Some(expected)));
    Ok(())
}

/// Test delta retrieval in the face of two concurrent transactions.
#[test]
fn multi_transaction_direct() -> Result<(), String> {
    fn do_test(
        observable1: UpdatesObservable,
        observable2: UpdatesObservable,
        observer: DDlogServer,
    ) -> Result<Box<dyn Any>, String> {
        let mut mux = TxnMux::new();
        let _ = mux.subscribe(Box::new(observer)).unwrap();
        let _ = mux.add_observable(Box::new(observable1)).unwrap();
        let _ = mux.add_observable(Box::new(observable2)).unwrap();

        Ok(Box::new(mux))
    }

    multi_transaction_test(do_test)
}

/// Test delta retrieval in the face of two concurrent transactions over
/// a TCP channel.
#[test]
fn multi_transaction_tcp() -> Result<(), String> {
    fn do_test(
        mut observable1: UpdatesObservable,
        mut observable2: UpdatesObservable,
        observer: DDlogServer,
    ) -> Result<Box<dyn Any>, String> {
        // We create the following setup:
        // o1 -> s1 --(TCP)-> r1 \
        //                        -- mux -> observer
        // o2 -> s2 --(TCP)-> r2 /
        //
        // (where oX=observableX, sX=sendX, and rX=recvX)

        let mut mux = TxnMux::new();
        let _ = mux.subscribe(Box::new(observer)).unwrap();

        let recv1 = TcpReceiver::new("127.0.0.1:0").unwrap();
        let send1 = TcpSender::connect(*recv1.addr()).unwrap();

        let recv2 = TcpReceiver::new("127.0.0.1:0").unwrap();
        let send2 = TcpSender::connect(*recv2.addr()).unwrap();

        let _ = observable1.subscribe(Box::new(send1)).unwrap();
        let _ = observable2.subscribe(Box::new(send2)).unwrap();

        let _ = mux.add_observable(Box::new(recv1)).unwrap();
        let _ = mux.add_observable(Box::new(recv2)).unwrap();

        Ok(Box::new(mux))
    }

    multi_transaction_test(do_test)
}
