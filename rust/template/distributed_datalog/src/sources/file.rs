use std::convert::TryFrom;
use std::fmt::Debug;
use std::fs::File as FsFile;
use std::io::BufRead;
use std::io::BufReader;
use std::iter::once;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::IntoRawFd;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::spawn;
use std::thread::JoinHandle;

use differential_datalog::DDlogInventory;
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

use crate::tcp_channel::Fd;
use crate::Observable;
use crate::Observer;
use crate::ObserverBox;

/// Handle the given command.
///
/// `updates` acts as an inter-procedural cache of updates that we will
/// push into the observer eventually.
fn handle<I>(
    id: usize,
    command: Command,
    updates: &mut Vec<Update<DDValue>>,
    observer: &mut dyn Observer<Update<DDValue>, String>,
    inventory: &I,
) where
    I: DDlogInventory,
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
        Command::Update(upd_cmd, last) => match upd_cmd.to_update(inventory) {
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

fn process<I>(
    id: usize,
    file: FsFile,
    fd: Arc<Fd>,
    mut observer: ObserverBox<Update<DDValue>, String>,
    inventory: &I,
) -> ObserverBox<Update<DDValue>, String>
where
    I: DDlogInventory,
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
                        handle(id, command, &mut updates, &mut observer, inventory);
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
pub struct File<I>
where
    I: DDlogInventory + Clone + Debug + Send + 'static,
{
    /// The file source's unique ID.
    id: usize,
    /// The path to the file we want to adapt to.
    path: PathBuf,
    /// The state we maintain.
    state: Option<State>,
    inventory: I,
}

impl<I> File<I>
where
    I: DDlogInventory + Clone + Debug + Send + 'static,
{
    /// Create a new adapter streaming data from the file at the given
    /// `path`.
    pub fn new<P>(path: P, inventory: I) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            id: Id::<()>::new().get(),
            path: path.into(),
            state: None,
            inventory,
        }
    }

    fn start(
        id: usize,
        path: &Path,
        observer: ObserverBox<Update<DDValue>, String>,
        inventory: I,
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
            thread: spawn(move || process(id, file, fd, observer, &inventory)),
        };

        Ok(state)
    }
}

impl<I> Drop for File<I>
where
    I: DDlogInventory + Clone + Debug + Send + 'static,
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

impl<I> Observable<Update<DDValue>, String> for File<I>
where
    I: DDlogInventory + Clone + Debug + Send + 'static,
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
            let state = Self::start(self.id, &self.path, observer, self.inventory.clone())?;
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

    use std::any::TypeId;
    #[cfg(feature = "c_api")]
    use std::ffi::CStr;
    use std::io::Write;
    use std::sync::Mutex;
    use std::thread::sleep;
    use std::time::Duration;

    use differential_datalog::program::{ArrId, IdxId, RelId};
    use differential_datalog::record::{Record, RelIdentifier};
    use differential_datalog_test::test_value::*;
    use fnv::FnvHashMap;
    use tempfile::NamedTempFile;
    use test_env_log::test;

    use crate::await_expected;
    use crate::MockObserver;
    use crate::SharedObserver;

    const TRANSACTION_DUMP: &[u8] = include_bytes!("file_test.dat");

    #[derive(Debug, Clone)]
    struct DummyInventory;

    impl DDlogInventory for DummyInventory {
        fn get_table_id(&self, tname: &str) -> Result<RelId, std::string::String> {
            todo!()
        }

        fn get_table_name(&self, tid: RelId) -> Result<&'static str, std::string::String> {
            todo!()
        }

        fn get_table_original_name(
            &self,
            tname: &str,
        ) -> Result<&'static str, std::string::String> {
            todo!()
        }

        #[cfg(feature = "c_api")]
        fn get_table_original_cname(
            &self,
            tname: &str,
        ) -> Result<&'static CStr, std::string::String> {
            todo!()
        }

        #[cfg(feature = "c_api")]
        fn get_table_cname(&self, tid: RelId) -> Result<&'static CStr, std::string::String> {
            todo!()
        }

        fn get_index_id(&self, iname: &str) -> Result<IdxId, std::string::String> {
            todo!()
        }

        fn get_index_name(&self, iid: IdxId) -> Result<&'static str, std::string::String> {
            todo!()
        }

        #[cfg(feature = "c_api")]
        fn get_index_cname(&self, iid: IdxId) -> Result<&'static CStr, std::string::String> {
            todo!()
        }

        fn input_relation_ids(&self) -> &'static FnvHashMap<RelId, &'static str> {
            todo!()
        }

        fn index_from_record(
            &self,
            index: IdxId,
            key: &Record,
        ) -> Result<DDValue, std::string::String> {
            todo!()
        }

        fn relation_type_id(&self, relation: RelId) -> Option<TypeId> {
            todo!()
        }

        fn relation_value_from_record(
            &self,
            relation: &RelIdentifier,
            value: &Record,
        ) -> Result<(RelId, DDValue), std::string::String> {
            todo!()
        }

        fn relation_key_from_record(
            &self,
            relation: &RelIdentifier,
            key: &Record,
        ) -> Result<(RelId, DDValue), std::string::String> {
            todo!()
        }

        fn index_to_arrangement_id(&self, index: IdxId) -> Option<ArrId> {
            todo!()
        }
    }

    #[test]
    fn non_existent_file() {
        let mock = SharedObserver::new(Mutex::new(MockObserver::new()));
        let mut adapter = File::new("i-dont-actually-exist", DummyInventory);
        let result = adapter.subscribe(Box::new(mock.clone()));

        assert!(result.is_err(), result);
    }

    #[test]
    fn source_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(TRANSACTION_DUMP).unwrap();
        file.flush().unwrap();

        let mock = SharedObserver::new(Mutex::new(MockObserver::new()));
        let mut adapter = File::new(file.path(), DummyInventory);
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
