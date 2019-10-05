use differential_datalog::record::{Record, RelIdentifier, UpdCmd};
use observe::Observable;
use observe::Observer;
use observe::SharedObserver;
use observe::Subscription;

use server_api_ddlog::api::*;
use server_api_ddlog::server::DDlogServer;
use server_api_ddlog::server::UpdatesObservable;
use server_api_ddlog::Relations::*;

use maplit::hashmap;
use maplit::hashset;

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

    {
        let mock = observer.0.lock().unwrap();
        assert_eq!(mock.called_on_start, 1);
        assert_eq!(mock.called_on_commit, 1);
    }

    server.shutdown()?;

    // Also verify that on_completed is called as part of the shutdown
    // procedure.
    {
        let mock = observer.0.lock().unwrap();
        assert_eq!(mock.called_on_completed, 1);
    }

    server.shutdown()?;

    // But only once!
    {
        let mock = observer.0.lock().unwrap();
        assert_eq!(mock.called_on_completed, 1);
    }
    Ok(())
}

/// Verify that we receive an `on_updates` (and `on_next`) call when we
/// get an update.
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

    let mock = observer.0.lock().unwrap();
    assert_eq!(mock.called_on_start, 1);
    assert_eq!(mock.called_on_updates, 1);
    assert_eq!(mock.called_on_commit, 1);
    Ok(())
}

/// Test `unsubscribe` functionality.
fn unsubscribe(
    mut server: DDlogServer,
    mut subscription: Box<dyn Subscription>,
    observer: MockObserver,
) -> Result<(), String> {
    server.on_start()?;
    server.on_commit()?;

    subscription.unsubscribe();

    server.on_start()?;
    server.on_commit()?;

    let mock = observer.0.lock().unwrap();
    assert_eq!(mock.called_on_start, 1);
    assert_eq!(mock.called_on_commit, 1);
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

    let mock = observer.0.lock().unwrap();
    assert_eq!(mock.called_on_start, 1);
    assert_eq!(mock.called_on_updates, 1);
    assert_eq!(mock.called_on_commit, 1);
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

    {
        let mock = observer.0.lock().unwrap();
        assert_eq!(mock.called_on_start, 1);
        assert_eq!(mock.called_on_updates, 2);
        assert_eq!(mock.called_on_commit, 1);
    }

    let updates = &[UpdCmd::Delete(
        RelIdentifier::RelId(P1In as usize),
        Record::String("first".to_string()),
    )];

    server.on_start()?;
    server.on_updates(Box::new(
        updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
    ))?;
    server.on_commit()?;

    {
        let mock = observer.0.lock().unwrap();
        assert_eq!(mock.called_on_start, 2);
        assert_eq!(mock.called_on_updates, 3);
        assert_eq!(mock.called_on_commit, 2);
    }

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
}
