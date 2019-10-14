use std::any::Any;
use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;
use std::panic::UnwindSafe;
use std::time::Duration;

use differential_datalog::program::Update;
use differential_datalog::record::Record;
use differential_datalog::record::RelIdentifier;
use differential_datalog::record::UpdCmd;
use distributed_datalog::Observable;
use distributed_datalog::Observer;
use distributed_datalog::SharedObserver;
use distributed_datalog::Subscription;
use distributed_datalog::TcpReceiver;
use distributed_datalog::TcpSender;

use server_api_ddlog::api::updcmd2upd;
use server_api_ddlog::api::HDDlog;
use server_api_ddlog::server::DDlogServer;
use server_api_ddlog::Relations::*;
use server_api_ddlog::Value;

use maplit::hashmap;
use maplit::hashset;

use test_env_log::test;

use waitfor::wait_for;

#[derive(Clone, Debug)]
struct Mock {
    called_on_start: usize,
    called_on_commit: usize,
    called_on_updates: usize,
    called_on_completed: usize,
}

impl Mock {
    fn new() -> Self {
        Self {
            called_on_start: 0,
            called_on_commit: 0,
            called_on_updates: 0,
            called_on_completed: 0,
        }
    }
}

impl<T, E> Observer<T, E> for Mock
where
    T: Send,
    E: Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        self.called_on_start += 1;
        Ok(())
    }

    fn on_commit(&mut self) -> Result<(), E> {
        self.called_on_commit += 1;
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        self.called_on_updates += updates.count();
        Ok(())
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.called_on_completed += 1;
        Ok(())
    }
}

type MockObserver = SharedObserver<Mock>;

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
    _subscription: Box<dyn Subscription>,
    observer: MockObserver,
) -> Result<(), String> {
    server.on_start()?;
    server.on_commit()?;
    server.on_completed()?;

    await_expected(|| {
        let (on_start, on_updates, on_commit) = {
            let mock = observer.0.lock().unwrap();
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
            let mock = observer.0.lock().unwrap();
            mock.called_on_completed
        };
        assert!(on_completed >= 1, "{}", on_completed);
    });

    server.shutdown()?;

    // But only once!
    await_expected(|| {
        let on_completed = {
            let mock = observer.0.lock().unwrap();
            mock.called_on_completed
        };
        assert!(on_completed >= 1, "{}", on_completed);
    });
    Ok(())
}

/// Verify that we receive an `on_updates` call when we get an update.
fn start_commit_with_updates(
    mut server: DDlogServer,
    _subscription: Box<dyn Subscription>,
    observer: MockObserver,
) -> Result<(), String> {
    let updates = &[UpdCmd::Insert(
        RelIdentifier::RelId(P1In as usize),
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
            let mock = observer.0.lock().unwrap();
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
    subscription: Box<dyn Subscription>,
    observer: MockObserver,
) -> Result<(), String> {
    server.on_start()?;
    server.on_commit()?;

    subscription.unsubscribe();

    server.on_start()?;
    server.on_commit()?;

    await_expected(|| {
        let (on_start, on_commit) = {
            let mock = observer.0.lock().unwrap();
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
    _subscription: Box<dyn Subscription>,
    observer: MockObserver,
) -> Result<(), String> {
    let updates = &[
        UpdCmd::Insert(
            RelIdentifier::RelId(P1In as usize),
            Record::String("42".to_string()),
        ),
        UpdCmd::Insert(
            RelIdentifier::RelId(P1In as usize),
            Record::String("here-to-stay".to_string()),
        ),
        UpdCmd::Delete(
            RelIdentifier::RelId(P1In as usize),
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
            let mock = observer.0.lock().unwrap();
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
    _subscription: Box<dyn Subscription>,
    observer: MockObserver,
) -> Result<(), String> {
    let updates = &[
        UpdCmd::Insert(
            RelIdentifier::RelId(P1In as usize),
            Record::String("first".to_string()),
        ),
        UpdCmd::Insert(
            RelIdentifier::RelId(P1In as usize),
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
            let mock = observer.0.lock().unwrap();
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
        RelIdentifier::RelId(P1In as usize),
        Record::String("first".to_string()),
    )];

    server.on_start()?;
    server.on_updates(Box::new(
        updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
    ))?;
    server.on_commit()?;

    await_expected(|| {
        let (on_start, on_updates, on_commit) = {
            let mock = observer.0.lock().unwrap();
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

fn setup() -> (DDlogServer, Box<dyn Subscription>, MockObserver) {
    let program = HDDlog::run(1, false, |_, _: &Record, _| {});
    let mut server = DDlogServer::new(program, hashmap! {});

    let observer = SharedObserver::new(Mock::new());
    let mut stream = server.add_stream(hashset! {P1Out});
    let subscription = stream.subscribe(Box::new(observer.clone())).unwrap();

    (server, subscription, observer)
}

fn setup_tcp() -> (
    DDlogServer,
    Box<dyn Subscription>,
    MockObserver,
    Box<dyn Any>,
) {
    let program = HDDlog::run(1, false, |_, _: &Record, _| {});
    let mut server = DDlogServer::new(program, hashmap! {});

    let observer = SharedObserver::new(Mock::new());
    let mut stream = server.add_stream(hashset! {P1Out});

    let mut recv = TcpReceiver::<Update<Value>>::new("127.0.0.1:0").unwrap();
    let send = TcpSender::connect(*recv.addr()).unwrap();

    let subscription = stream.subscribe(Box::new(send)).unwrap();
    let _ = recv.subscribe(Box::new(observer.clone())).unwrap();

    (server, subscription, observer, Box::new(recv))
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
