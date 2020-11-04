use std::fs::File as FsFile;
use std::marker::PhantomData;

use log::trace;
use uid::Id;

use differential_datalog::ddval::DDValue;
use differential_datalog::program::Update;
use differential_datalog::record_val_upds;
use differential_datalog::DDlogConvert;
use differential_datalog::RecordReplay;

use crate::Observer;

/// An object implementing the `Observer` interface and dumping
/// transaction traces into a file.
#[derive(Debug)]
pub struct File<C> {
    /// The file sink's unique ID.
    id: usize,
    /// The file we dump our transaction traces into.
    file: FsFile,
    /// Unused phantom data.
    _unused: PhantomData<C>,
}

impl<C> File<C> {
    /// Create a new file based observer using the given file.
    pub fn new(file: FsFile) -> Self {
        let id = Id::<()>::new().get();
        trace!("File({})::new", id);

        Self {
            id,
            file,
            _unused: PhantomData,
        }
    }
}

impl<C> Observer<Update<DDValue>, String> for File<C>
where
    C: Send + DDlogConvert,
{
    fn on_start(&mut self) -> Result<(), String> {
        trace!("File({})::on_start", self.id);

        self.file
            .record_start()
            .map_err(|e| format!("failed to record 'on_start' event: {}", e))
    }

    fn on_commit(&mut self) -> Result<(), String> {
        trace!("File({})::on_commit", self.id);

        self.file
            .record_commit(false)
            .map_err(|e| format!("failed to record 'on_commit' event: {}", e))
    }

    fn on_updates<'a>(
        &mut self,
        updates: Box<dyn Iterator<Item = Update<DDValue>> + 'a>,
    ) -> Result<(), String> {
        trace!("File({})::on_updates", self.id);

        let mut result = Ok(());
        record_val_upds::<C, _, _, _>(&mut self.file, updates, |e| {
            result = Err(e);
        })
        .for_each(|_| ());

        result.map_err(|e| format!("failed to record 'on_updates' event(s): {}", e))
    }

    fn on_completed(&mut self) -> Result<(), String> {
        trace!("File({})::on_completed", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Read;

    use tempfile::NamedTempFile;

    use differential_datalog::ddval::DDValConvert;
    use differential_datalog::program::IdxId;
    use differential_datalog::program::RelId;
    use differential_datalog::record::Record;
    use differential_datalog::record::RelIdentifier;
    use differential_datalog::record::UpdCmd;
    use differential_datalog_test::test_value::*;

    #[derive(Debug)]
    struct DummyConverter;

    impl DDlogConvert for DummyConverter {
        fn relid2name(rel_id: RelId) -> Option<&'static str> {
            match rel_id {
                1 => Some("test_rel"),
                _ => panic!("unexpected RelId {}", rel_id),
            }
        }

        fn indexid2name(idx_id: IdxId) -> Option<&'static str> {
            panic!("unexpected IdxId {}", idx_id)
        }

        fn updcmd2upd(upd_cmd: &UpdCmd) -> Result<Update<DDValue>, std::string::String> {
            match upd_cmd {
                UpdCmd::Insert(relident, record) => {
                    let relid = match relident {
                        RelIdentifier::RelId(relid) => *relid,
                        _ => panic!(
                            "encountered unexpected RelIdentifier variant: {:?}",
                            relident
                        ),
                    };
                    let v = match record {
                        Record::String(string) => String(string.clone()).into_ddvalue(),
                        _ => panic!("encountered unexpected Record variant: {:?}", record),
                    };
                    Ok(Update::Insert { relid, v })
                }
                _ => panic!("unsupported UpdCmd: {:?}", upd_cmd),
            }
        }
    }

    #[test]
    fn dump_events() {
        let tempfile = NamedTempFile::new().unwrap();
        let mut file = tempfile.reopen().unwrap();
        let mut sink = File::<DummyConverter>::new(tempfile.into_file());
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
