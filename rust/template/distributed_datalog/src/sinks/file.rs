use differential_datalog::ddval::DDValue;
use differential_datalog::program::Update;
use differential_datalog::{CommandRecorder, DDlog, DDlogDynamic, DDlogInventory};
use log::trace;
use std::fs::File as FsFile;
use std::sync::Arc;
use uid::Id;

use crate::Observer;

/// An object implementing the `Observer` interface and dumping
/// transaction traces into a file.
#[derive(Debug)]
pub struct File {
    /// The file sink's unique ID.
    id: usize,
    /// The file-backed recorder we dump our transaction traces into.
    recorder: CommandRecorder<FsFile, Arc<dyn DDlogInventory + Send + Sync>>,
}

impl File {
    /// Create a new file-based observer using the given file.
    pub fn new(file: FsFile, inventory: Arc<dyn DDlogInventory + Send + Sync>) -> Self {
        let id = Id::<()>::new().get();
        trace!("File({})::new", id);

        Self {
            id,
            recorder: CommandRecorder::new(file, inventory),
        }
    }
}

impl Observer<Update<DDValue>, String> for File {
    fn on_start(&mut self) -> Result<(), String> {
        trace!("File({})::on_start", self.id);

        self.recorder
            .transaction_start()
            .map_err(|e| format!("failed to record 'on_start' event: {}", e))
    }

    fn on_commit(&mut self) -> Result<(), String> {
        trace!("File({})::on_commit", self.id);

        self.recorder
            .transaction_commit()
            .map_err(|e| format!("failed to record 'on_commit' event: {}", e))
    }

    fn on_updates<'a>(
        &mut self,
        mut updates: Box<dyn Iterator<Item = Update<DDValue>> + 'a>,
    ) -> Result<(), String> {
        trace!("File({})::on_updates", self.id);

        self.recorder
            .apply_updates(&mut *updates)
            .map_err(|e| format!("failed to record 'on_updates' event(s): {}", e))
    }

    fn on_completed(&mut self) -> Result<(), String> {
        trace!("File({})::on_completed", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ffi::CStr;
    use std::io::Read;
    use tempfile::NamedTempFile;

    use differential_datalog::ddval::DDValConvert;
    use differential_datalog::program::IdxId;
    use differential_datalog::program::RelId;
    use differential_datalog_test::test_value::*;

    #[derive(Debug)]
    struct DummyConverter;

    impl DDlogInventory for DummyConverter {
        fn get_table_id(&self, _tname: &str) -> Result<RelId, std::string::String> {
            Err("not implemented".to_string())
        }
        fn get_table_name(&self, tid: RelId) -> Result<&'static str, std::string::String> {
            match tid {
                1 => Ok("test_rel"),
                _ => panic!("unexpected RelId {}", tid),
            }
        }
        fn get_table_cname(&self, _tid: RelId) -> Result<&'static CStr, std::string::String> {
            Err("not implemented".to_string())
        }
        fn get_index_id(&self, _iname: &str) -> Result<IdxId, std::string::String> {
            Err("not implemented".to_string())
        }
        fn get_index_name(&self, _iid: IdxId) -> Result<&'static str, std::string::String> {
            Err("not implemented".to_string())
        }
        fn get_index_cname(&self, _iid: IdxId) -> Result<&'static CStr, std::string::String> {
            Err("not implemented".to_string())
        }
    }

    #[test]
    fn dump_events() {
        let tempfile = NamedTempFile::new().unwrap();
        let mut file = tempfile.reopen().unwrap();
        let mut sink = File::new(tempfile.into_file(), Arc::new(DummyConverter));
        let observer = &mut sink as &mut dyn Observer<Update<DDValue>, _>;

        observer.on_start().unwrap();
        observer.on_commit().unwrap();

        let updates = vec![5, 4, 9, 1, 2, 3]
            .into_iter()
            .map(|val| Update::Insert {
                relid: 1,
                v: String(val.to_string()).into_ddvalue(),
            });

        observer.on_start().unwrap();
        observer.on_updates(Box::new(updates)).unwrap();
        observer.on_commit().unwrap();

        let mut content = std::string::String::new();
        let _ = file.read_to_string(&mut content).unwrap();
        let expected = r#"start;
commit;
start;
insert test_rel["5"],
insert test_rel["4"],
insert test_rel["9"],
insert test_rel["1"],
insert test_rel["2"],
insert test_rel["3"];
commit;
"#;
        assert_eq!(content, expected);
    }
}
