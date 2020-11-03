use std::convert::TryFrom;
use std::fs::File as FsFile;
use std::io::BufRead;
use std::io::BufReader;
use std::iter::once;
use std::marker::PhantomData;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::IntoRawFd;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::spawn;
use std::thread::JoinHandle;

use libc::c_uint;
use log::error;
use log::info;
use log::trace;
use nom::Err;
use uid::Id;

use cmd_parser::err_str;
use cmd_parser::parse_command;
use cmd_parser::Command;
use differential_datalog::ddval::DDValue;
use differential_datalog::program::Update;
use differential_datalog::DDlogConvert;

use crate::tcp_channel::Fd;
use crate::Observable;
use crate::Observer;
use crate::ObserverBox;

/// Handle the given command.
///
/// `updates` acts as an inter-procedural cache of updates that we will
/// push into the observer eventually.
fn handle<C>(
    id: usize,
    command: Command,
    updates: &mut Vec<Update<DDValue>>,
    observer: &mut dyn Observer<Update<DDValue>, String>,
) where
    C: DDlogConvert,
{
    trace!("File({})::handle: {:?}", id, command);

    match command {
        Command::Start => {
            let _ = observer
                .on_start()
                .map_err(|e| error!("observer failed on_start: {:?}", e));
        }
        Command::Commit(_) => {
            let _ = observer
                .on_commit()
                .map_err(|e| error!("observer failed on_commit: {:?}", e));
        }
        Command::Update(upd_cmd, last) => match C::updcmd2upd(&upd_cmd) {
            Ok(upd) => {
                if last {
                    let updates = updates.drain(..).chain(once(upd));
                    let _ = observer
                        .on_updates(Box::new(updates))
                        .map_err(|e| error!("observer failed on_updates: {:?}", e));
                } else {
                    updates.push(upd)
                }
            }
            Err(e) => error!("failed to convert UpdCmd to Update: {}", e),
        },
        Command::Exit => {
            let _ = observer
                .on_completed()
                .map_err(|e| error!("observer failed on_completed: {:?}", e));
        }
        // TODO: Eventually we will need to add support for the 'Clear'
        //       command.
        _ => info!("ignoring unsupported command: {:?}", command),
    }
}

fn process<C>(
    id: usize,
    file: FsFile,
    fd: Arc<Fd>,
    mut observer: ObserverBox<Update<DDValue>, String>,
) -> ObserverBox<Update<DDValue>, String>
where
    C: DDlogConvert,
{
    // TODO: The logic here somewhat resembles that in
    //       `cmd_parser/lib.rs`. We may want to deduplicate at some
    //       point.
    let mut buffer = Vec::new();
    let mut updates = Vec::new();
    let mut reader = BufReader::new(file);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            // TODO: We need to handle the Ok(0) case which occurs if
            //       the underlying file has reached EOF. We basically
            //       would need to register a poll(2) for the fd or
            //       something along those lines.
            Ok(_) => {
                buffer.extend_from_slice(line.as_bytes());
                match parse_command(buffer.as_slice()) {
                    Ok((rest, command)) => {
                        // TODO: Remove unnecessary allocation. (Replace
                        //       with Vec::splice perhaps?)
                        buffer = rest.to_owned();
                        handle::<C>(id, command, &mut updates, &mut observer);
                    }
                    Err(Err::Incomplete(_)) => (),
                    Err(e) => {
                        error!("encountered invalid input: {}", err_str(&e));
                        buffer.clear();
                    }
                }
            }
            Err(e) => {
                if fd.is_closed() {
                    // Because `fd` has been closed, we should not
                    // close the file object wrapped by our reader
                    // again, or we may close an unrelated file
                    // descriptor that happens to reuse the same number.
                    let _ = reader.into_inner().into_raw_fd();
                    return observer;
                }
                error!("failed to read from source file: {}", e);
            }
        }
    }
}

#[derive(Debug)]
struct State {
    /// The raw handle of the file we are reading from.
    fd: Arc<Fd>,
    /// The thread reading from the file.
    thread: JoinHandle<ObserverBox<Update<DDValue>, String>>,
}

/// An object adapting a file to the `Observable` interface.
#[derive(Debug)]
pub struct File<C>
where
    C: DDlogConvert,
{
    /// The file source's unique ID.
    id: usize,
    /// The path to the file we want to adapt to.
    path: PathBuf,
    /// The state we maintain.
    state: Option<State>,
    /// Unused phantom data.
    _unused: PhantomData<C>,
}

impl<C> File<C>
where
    C: DDlogConvert,
{
    /// Create a new adapter streaming data from the file at the given
    /// `path`.
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            id: Id::<()>::new().get(),
            path: path.into(),
            state: None,
            _unused: Default::default(),
        }
    }

    fn start(
        id: usize,
        path: &Path,
        observer: ObserverBox<Update<DDValue>, String>,
    ) -> Result<State, ObserverBox<Update<DDValue>, String>> {
        let file = match FsFile::open(path) {
            Ok(file) => file,
            Err(e) => {
                error!("failed to open file {}: {}", path.display(), e);
                return Err(observer);
            }
        };
        let fd = c_uint::try_from(file.as_raw_fd()).unwrap();
        let fd = Arc::new(Fd::new(fd));
        let state = State {
            fd: fd.clone(),
            thread: spawn(move || process::<C>(id, file, fd, observer)),
        };

        Ok(state)
    }
}

impl<C> Drop for File<C>
where
    C: DDlogConvert,
{
    fn drop(&mut self) {
        let _ = self.unsubscribe(&());
        if let Some(state) = self.state.take() {
            if let Err(e) = state.fd.close() {
                error!("failed to close adapted file: {}", e);
                // We continue as there is not much we can do about
                // the error. There shouldn't be any chance that we
                // actually fail the close while the thread is still
                // blocking on a read, but who knows.
            }
        }
    }
}

impl<C> Observable<Update<DDValue>, String> for File<C>
where
    C: DDlogConvert,
{
    type Subscription = ();

    fn subscribe(
        &mut self,
        observer: ObserverBox<Update<DDValue>, String>,
    ) -> Result<Self::Subscription, ObserverBox<Update<DDValue>, String>> {
        trace!("File({})::subscribe", self.id);

        if self.state.is_some() {
            Err(observer)
        } else {
            let state = Self::start(self.id, &self.path, observer)?;
            let _ = self.state.replace(state);
            Ok(())
        }
    }

    fn unsubscribe(
        &mut self,
        _subscription: &Self::Subscription,
    ) -> Option<ObserverBox<Update<DDValue>, String>> {
        trace!("File({})::unsubscribe", self.id);

        if let Some(state) = self.state.take() {
            if let Err(e) = state.fd.close() {
                error!("failed to close adapted file: {}", e);
                // We continue as there is not much we can do about
                // the error. There shouldn't be any chance that we
                // actually fail the close while the thread is still
                // blocking on a read, but who knows.
            }
            match state.thread.join() {
                Ok(observer) => Some(observer),
                Err(e) => {
                    error!("file observer thread panicked: {:?}", e);
                    None
                }
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;
    use std::sync::Mutex;
    use std::thread::sleep;
    use std::time::Duration;

    use tempfile::NamedTempFile;
    use test_env_log::test;

    use differential_datalog::program::IdxId;
    use differential_datalog::program::RelId;
    use differential_datalog::record::UpdCmd;
    use differential_datalog_test::test_value::*;
    use types::ddval_convert::DDValConvert;

    use crate::await_expected;
    use crate::MockObserver;
    use crate::SharedObserver;

    const TRANSACTION_DUMP: &[u8] = include_bytes!("file_test.dat");

    #[derive(Debug)]
    struct DummyConverter;

    impl DDlogConvert for DummyConverter {
        fn relid2name(_rel_id: RelId) -> Option<&'static str> {
            unimplemented!()
        }

        fn indexid2name(_idx_id: IdxId) -> Option<&'static str> {
            unimplemented!()
        }

        fn updcmd2upd(_upd_cmd: &UpdCmd) -> Result<Update<DDValue>, std::string::String> {
            // Exact details do not matter in this context, so just fake
            // some data.
            Ok(Update::Insert {
                relid: 0,
                v: Empty {}.into_ddvalue(),
            })
        }
    }

    #[test]
    fn non_existent_file() {
        let mock = SharedObserver::new(Mutex::new(MockObserver::new()));
        let mut adapter = File::<DummyConverter>::new("i-dont-actually-exist");
        let result = adapter.subscribe(Box::new(mock.clone()));

        assert!(result.is_err(), result);
    }

    #[test]
    fn source_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(TRANSACTION_DUMP).unwrap();
        file.flush().unwrap();

        let mock = SharedObserver::new(Mutex::new(MockObserver::new()));
        let mut adapter = File::<DummyConverter>::new(file.path());
        let _ = adapter.subscribe(Box::new(mock.clone())).unwrap();

        await_expected(|| {
            let (on_start, on_updates, on_commit) = {
                let guard = mock.lock().unwrap();
                (
                    guard.called_on_start,
                    guard.called_on_updates,
                    guard.called_on_commit,
                )
            };
            assert_eq!(on_start, 2);
            assert_eq!(on_updates, 5);
            assert_eq!(on_commit, 2);
        });

        // Add the same data into the file and verify that we get more
        // updates.
        file.write_all(TRANSACTION_DUMP).unwrap();
        file.flush().unwrap();

        await_expected(|| {
            let (on_start, on_updates, on_commit) = {
                let guard = mock.lock().unwrap();
                (
                    guard.called_on_start,
                    guard.called_on_updates,
                    guard.called_on_commit,
                )
            };
            assert_eq!(on_start, 4);
            assert_eq!(on_updates, 10);
            assert_eq!(on_commit, 4);
        });

        // Unsubscribe and verify that additional data does not result
        // in more updates.
        let _ = adapter.unsubscribe(&()).unwrap();

        file.write_all(TRANSACTION_DUMP).unwrap();
        file.flush().unwrap();
        sleep(Duration::from_millis(250));

        assert_eq!(mock.lock().unwrap().called_on_start, 4);
    }
}
