use std::any::Any;
use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;
use std::panic::UnwindSafe;
use std::sync::Mutex;
use std::time::Duration;

use differential_datalog::program::Update;
use differential_datalog::record::Record;
use differential_datalog::record::RelIdentifier;
use differential_datalog::record::UpdCmd;
use differential_datalog::DDlog;
use distributed_datalog::DDlogServer as DDlogServerT;
use distributed_datalog::MockObserver as Mock;
use distributed_datalog::Observable;
use distributed_datalog::Observer;
use distributed_datalog::SharedObserver;
use distributed_datalog::TcpReceiver;
use distributed_datalog::TcpSender;
use distributed_datalog::UpdatesObservable as UpdatesObservableT;

use server_api_ddlog::api::updcmd2upd;
use server_api_ddlog::api::HDDlog;
use server_api_ddlog::Relations::*;
use server_api_ddlog::Value;

use maplit::hashmap;
use maplit::hashset;

use test_env_log::test;

use waitfor::wait_for;

type DDlogServer = DDlogServerT<HDDlog>;
type MockObserver = SharedObserver<Mock>;
type UpdatesObservable = UpdatesObservableT<Update<Value>, String>;

fn await_expected<F>(mut op: F)
where
    F: FnMut() + UnwindSafe,
{
    let op = || -> Result<Option<()>, ()> {
        // All we care about for the sake of testing here are assertion
        // failures. So if we encounter one (in the form of a panic) we
        // map that to a retry.
        match catch_unwind(AssertUnwindSafe(&mut op)) {
            Ok(_) => Ok(Some(())),
            Err(_) => Ok(None),
        }
    };

    let result = wait_for(Duration::from_secs(5), Duration::from_millis(1), op);
    match result {
        Ok(Some(_)) => (),
        Ok(None) => panic!("time out waiting for expected result"),
        Err(e) => panic!("failed to await expected result: {:?}", e),
    }
}

/// Verify that `on_commit` is called even if we haven't received any
/// updates.
fn start_commit_on_no_updates(
    mut server: DDlogServer,
    _observable: UpdatesObservable,
    observer: MockObserver,
) -> Result<(), String> {
    server.on_start()?;
    server.on_commit()?;
    server.on_completed()?;

    await_expected(|| {
        let (on_start, on_updates, on_commit) = {
            let mock = observer.lock().unwrap();
            (
                mock.called_on_start,
                mock.called_on_updates,
                mock.called_on_commit,
            )
        };

        assert_eq!(on_start, 1);
        assert_eq!(on_updates, 0);
        assert_eq!(on_commit, 1);
    });

    server.shutdown()?;

    // Also verify that on_completed is called as part of the shutdown
    // procedure.
    await_expected(|| {
        let on_completed = {
            let mock = observer.lock().unwrap();
            mock.called_on_completed
        };
        assert!(on_completed >= 1, "{}", on_completed);
    });

    server.shutdown()?;

    // But only once!
    await_expected(|| {
        let on_completed = {
            let mock = observer.lock().unwrap();
            mock.called_on_completed
        };
        assert!(on_completed >= 1, "{}", on_completed);
    });
    Ok(())
}

/// Verify that we receive an `on_updates` call when we get an update.
fn start_commit_with_updates(
    mut server: DDlogServer,
    _observable: UpdatesObservable,
    observer: MockObserver,
) -> Result<(), String> {
    let updates = &[UpdCmd::Insert(
        RelIdentifier::RelId(server_api_1_P1In as usize),
        Record::String("test".to_string()),
    )];

    server.on_start()?;
    server.on_updates(Box::new(
        updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
    ))?;
    server.on_commit()?;
    server.on_completed()?;

    await_expected(|| {
        let (on_start, on_updates, on_commit) = {
            let mock = observer.lock().unwrap();
            (
                mock.called_on_start,
                mock.called_on_updates,
                mock.called_on_commit,
            )
        };

        assert_eq!(on_start, 1);
        assert_eq!(on_updates, 1);
        assert_eq!(on_commit, 1);
    });
    Ok(())
}

/// Test `unsubscribe` functionality.
fn unsubscribe(
    mut server: DDlogServer,
    mut observable: UpdatesObservable,
    observer: MockObserver,
) -> Result<(), String> {
    server.on_start()?;
    server.on_commit()?;

    assert!(observable.unsubscribe(&()).is_some());
    assert!(observable.unsubscribe(&()).is_none());

    server.on_start()?;
    server.on_commit()?;

    await_expected(|| {
        let (on_start, on_commit) = {
            let mock = observer.lock().unwrap();
            (mock.called_on_start, mock.called_on_commit)
        };

        assert_eq!(on_start, 1);
        assert_eq!(on_commit, 1);
    });
    Ok(())
}

/// Verify that we do not receive repeated `on_next` calls for inserts &
/// deletes of the same object within a single transaction.
fn multiple_mergable_updates(
    mut server: DDlogServer,
    _observable: UpdatesObservable,
    observer: MockObserver,
) -> Result<(), String> {
    let updates = &[
        UpdCmd::Insert(
            RelIdentifier::RelId(server_api_1_P1In as usize),
            Record::String("42".to_string()),
        ),
        UpdCmd::Insert(
            RelIdentifier::RelId(server_api_1_P1In as usize),
            Record::String("here-to-stay".to_string()),
        ),
        UpdCmd::Delete(
            RelIdentifier::RelId(server_api_1_P1In as usize),
            Record::String("42".to_string()),
        ),
    ];

    server.on_start()?;
    server.on_updates(Box::new(
        updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
    ))?;
    server.on_commit()?;
    server.on_completed()?;

    await_expected(|| {
        let (on_start, on_updates, on_commit) = {
            let mock = observer.lock().unwrap();
            (
                mock.called_on_start,
                mock.called_on_updates,
                mock.called_on_commit,
            )
        };

        assert_eq!(on_start, 1);
        assert_eq!(on_updates, 1);
        assert_eq!(on_commit, 1);
    });
    Ok(())
}

/// Check intended behavior in the context of multiple transactions
/// happening.
fn multiple_transactions(
    mut server: DDlogServer,
    _observable: UpdatesObservable,
    observer: MockObserver,
) -> Result<(), String> {
    let updates = &[
        UpdCmd::Insert(
            RelIdentifier::RelId(server_api_1_P1In as usize),
            Record::String("first".to_string()),
        ),
        UpdCmd::Insert(
            RelIdentifier::RelId(server_api_1_P1In as usize),
            Record::String("second".to_string()),
        ),
    ];

    server.on_start()?;
    server.on_updates(Box::new(
        updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
    ))?;
    server.on_commit()?;

    await_expected(|| {
        let (on_start, on_updates, on_commit) = {
            let mock = observer.lock().unwrap();
            (
                mock.called_on_start,
                mock.called_on_updates,
                mock.called_on_commit,
            )
        };

        assert_eq!(on_start, 1);
        assert_eq!(on_updates, 2);
        assert_eq!(on_commit, 1);
    });

    let updates = &[UpdCmd::Delete(
        RelIdentifier::RelId(server_api_1_P1In as usize),
        Record::String("first".to_string()),
    )];

    server.on_start()?;
    server.on_updates(Box::new(
        updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
    ))?;
    server.on_commit()?;

    await_expected(|| {
        let (on_start, on_updates, on_commit) = {
            let mock = observer.lock().unwrap();
            (
                mock.called_on_start,
                mock.called_on_updates,
                mock.called_on_commit,
            )
        };

        assert_eq!(on_start, 2);
        assert_eq!(on_updates, 3);
        assert_eq!(on_commit, 2);
    });

    server.on_completed()?;
    Ok(())
}

fn setup() -> (DDlogServer, UpdatesObservable, MockObserver) {
    let program = HDDlog::run(1, false, |_, _: &Record, _| {}).unwrap();
    let mut server = DDlogServer::new(program, hashmap! {});

    let observer = SharedObserver::new(Mutex::new(Mock::new()));
    let mut stream = server.add_stream(hashset! {server_api_1_P1Out as usize});
    let _ = stream.subscribe(Box::new(observer.clone())).unwrap();

    (server, stream, observer)
}

fn setup_tcp() -> (DDlogServer, UpdatesObservable, MockObserver, Box<dyn Any>) {
    let program = HDDlog::run(1, false, |_, _: &Record, _| {}).unwrap();
    let mut server = DDlogServer::new(program, hashmap! {});

    let observer = SharedObserver::new(Mutex::new(Mock::new()));
    let mut stream = server.add_stream(hashset! {server_api_1_P1Out as usize});

    let mut recv = TcpReceiver::<Update<Value>>::new("127.0.0.1:0").unwrap();
    let send = TcpSender::connect(*recv.addr()).unwrap();

    let _ = stream.subscribe(Box::new(send)).unwrap();
    let _ = recv.subscribe(Box::new(observer.clone())).unwrap();

    (server, stream, observer, Box::new(recv))
}

#[test]
fn all_events() {
    let tests = &[
        start_commit_on_no_updates,
        start_commit_with_updates,
        unsubscribe,
        multiple_mergable_updates,
        multiple_transactions,
    ];

    for test in tests {
        let (server, observable, observer) = setup();
        test(server, observable, observer).unwrap();
    }

    for test in tests {
        let (server, subscription, observer, _data) = setup_tcp();
        test(server, subscription, observer).unwrap();
    }
}
