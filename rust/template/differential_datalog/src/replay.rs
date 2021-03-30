use std::collections::{BTreeMap, BTreeSet};
use std::convert::AsRef;
use std::fmt::{Debug, Display, Formatter};
use std::io::Result as IOResult;
use std::io::Write;
use std::iter::Peekable;
use std::ops::Deref;
use std::string::ToString;
use std::sync::Mutex;

use crate::ddlog::{DDlog, DDlogDump, DDlogDynamic, DDlogInventory, DDlogProfiling};
use crate::ddval::DDValue;
use crate::program::IdxId;
use crate::program::RelId;
use crate::program::Update;
use crate::record::Record;
use crate::record::RelIdentifier;
use crate::record::UpdCmd;
use crate::DeltaMap;

/// A custom iterator that indicates in each yielded element whether it
/// is the last one or not.
struct Peeking<I>
where
    I: Iterator,
{
    iter: Peekable<I>,
}

impl<I> Peeking<I>
where
    I: Iterator,
{
    fn new(iter: I) -> Self {
        Self {
            iter: iter.peekable(),
        }
    }
}

impl<I> Iterator for Peeking<I>
where
    I: Iterator,
{
    /// The iterator yields tuples of items of the underlying iterator
    /// together with an enum indicating whether this is the last
    /// element.
    type Item = (I::Item, bool);

    fn next(&mut self) -> Option<(I::Item, bool)> {
        let elem = self.iter.next()?;
        let last = self.iter.peek().is_none();
        Some((elem, last))
    }
}

/// DDlog API implementation that records each command passing through it and
/// forwards it to the next handler in the chain.
///
/// Error handling:
/// If the next handler is specified, we invoke it, whether recording succeeded
/// or failed, and return the status returned by the next handler. Otherwise, we
/// return the error code produced by the writer.
pub struct CommandRecorder<W, I> {
    writer: Mutex<W>,
    // Typically, `I` is `Box<dyn DDlogInventory + Send + Sync>` or
    // `Arc<dyn DDlogInventory + Send + Sync>`
    inventory: I,
}

impl<W, B> Debug for CommandRecorder<W, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommandRecorder")
    }
}

impl<W, B> CommandRecorder<W, B> {
    pub fn new(writer: W, inventory: B) -> Self {
        CommandRecorder {
            writer: Mutex::new(writer),
            inventory,
        }
    }

    pub fn release_writer(self) -> W {
        self.writer.into_inner().unwrap()
    }

    /// Convert a `RelIdentifier` into its symbolic name.
    fn relident2name<'a>(
        inventory: &dyn DDlogInventory,
        rel_ident: &'a RelIdentifier,
    ) -> Option<&'a str> {
        match rel_ident {
            RelIdentifier::RelName(rname) => Some(rname.as_ref()),
            RelIdentifier::RelId(id) => inventory.get_table_name(*id).ok(),
        }
    }
}

pub fn record_insert<V>(writer: &mut dyn Write, name: &str, value: V) -> IOResult<()>
where
    V: Display,
{
    write!(writer, "insert {}[{}]", name, value)
}

pub fn record_delete<V>(writer: &mut dyn Write, name: &str, value: V) -> IOResult<()>
where
    V: Display,
{
    write!(writer, "delete {}[{}]", name, value)
}

pub fn record_insert_or_update<V>(writer: &mut dyn Write, name: &str, value: V) -> IOResult<()>
where
    V: Display,
{
    write!(writer, "insert_or_update {}[{}]", name, value)
}

impl<W, I> CommandRecorder<W, I>
where
    W: Write,
    I: Deref<Target = dyn DDlogInventory + Send + Sync>,
{
    fn do_record_updates<It, U, F>(&self, updates: It, mut record: F) -> Result<(), String>
    where
        W: Write,
        It: Iterator<Item = U>,
        F: FnMut(&dyn DDlogInventory, &mut W, &U) -> IOResult<()>,
    {
        let mut writer = self.writer.lock().unwrap();
        let inventory = &*self.inventory;
        Peeking::new(updates)
            .try_for_each(move |(upd, last)| {
                record(inventory, &mut writer, &upd).and_then(|_| {
                    if !last {
                        writeln!(&mut writer, ",")
                    } else {
                        writeln!(&mut writer, ";")
                    }
                })
            })
            .map_err(|e| e.to_string())
    }

    /// Record an `UpdCmd`.
    fn record_upd_cmd(
        inventory: &dyn DDlogInventory,
        writer: &mut W,
        upd: &UpdCmd,
    ) -> IOResult<()> {
        match upd {
            UpdCmd::Insert(rel, record) => record_insert(
                writer,
                Self::relident2name(inventory, rel).unwrap_or(&"???"),
                record,
            ),
            UpdCmd::InsertOrUpdate(rel, record) => record_insert_or_update(
                writer,
                Self::relident2name(inventory, rel).unwrap_or(&"???"),
                record,
            ),
            UpdCmd::Delete(rel, record) => record_delete(
                writer,
                Self::relident2name(inventory, rel).unwrap_or(&"???"),
                record,
            ),
            UpdCmd::DeleteKey(rel, record) => {
                let rname = Self::relident2name(inventory, rel).unwrap_or(&"???");
                write!(writer, "delete_key {} {}", rname, record,)
            }
            UpdCmd::Modify(rel, key, mutator) => {
                let rname = Self::relident2name(inventory, rel).unwrap_or(&"???");
                write!(writer, "modify {} {} <- {}", rname, key, mutator,)
            }
        }
    }

    /// Record an `Update`.
    fn record_val_upd(
        inventory: &dyn DDlogInventory,
        writer: &mut W,
        upd: &Update<DDValue>,
    ) -> IOResult<()> {
        match upd {
            Update::Insert { relid, v } => record_insert(
                writer,
                inventory.get_table_name(*relid).unwrap_or(&"???"),
                v,
            ),
            Update::InsertOrUpdate { relid, v } => record_insert_or_update(
                writer,
                inventory.get_table_name(*relid).unwrap_or(&"???"),
                v,
            ),
            Update::DeleteValue { relid, v } => record_delete(
                writer,
                inventory.get_table_name(*relid).unwrap_or(&"???"),
                v,
            ),
            Update::DeleteKey { relid, k } => write!(
                writer,
                "delete_key {} {}",
                inventory.get_table_name(*relid).unwrap_or(&"???"),
                k,
            ),
            Update::Modify { relid, k, m } => write!(
                writer,
                "modify {} {} <- {}",
                inventory.get_table_name(*relid).unwrap_or(&"???"),
                k,
                m,
            ),
        }
    }
}

impl<W, I> DDlogDynamic for CommandRecorder<W, I>
where
    W: Write,
    I: Deref<Target = dyn DDlogInventory + Send + Sync>,
{
    fn transaction_start(&self) -> Result<(), String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(&mut writer, "start;").map_err(|e| e.to_string())
    }

    fn transaction_commit(&self) -> Result<(), String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(&mut writer, "commit;").map_err(|e| e.to_string())
    }

    fn transaction_commit_dump_changes_dynamic(
        &self,
    ) -> Result<BTreeMap<RelId, Vec<(Record, isize)>>, String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(&mut writer, "commit dump_changes;")
            .map(|_| BTreeMap::new())
            .map_err(|e| e.to_string())
    }

    fn transaction_rollback(&self) -> Result<(), String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(&mut writer, "rollback;").map_err(|e| e.to_string())
    }

    fn apply_updates_dynamic(&self, upds: &mut dyn Iterator<Item = UpdCmd>) -> Result<(), String> {
        self.do_record_updates(upds, |i, w, u| Self::record_upd_cmd(i, w, &u))
    }

    fn clear_relation(&self, rid: RelId) -> Result<(), String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(
            &mut writer,
            "clear {};",
            self.inventory.get_table_name(rid).unwrap_or(&"???")
        )
        .map_err(|e| e.to_string())
    }

    fn query_index_dynamic(&self, iid: IdxId, key: &Record) -> Result<Vec<Record>, String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(
            &mut writer,
            "query_index {}({});",
            self.inventory.get_index_name(iid).unwrap_or(&"???"),
            key
        )
        .map(|_| vec![])
        .map_err(|e| e.to_string())
    }

    fn dump_index_dynamic(&self, iid: IdxId) -> Result<Vec<Record>, String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(
            &mut writer,
            "dump_index {};",
            self.inventory.get_index_name(iid).unwrap_or(&"???")
        )
        .map(|_| vec![])
        .map_err(|e| e.to_string())
    }

    fn stop(&self) -> Result<(), String> {
        Ok(())
    }
}

impl<W, I> DDlog for CommandRecorder<W, I>
where
    W: Write,
    I: Deref<Target = dyn DDlogInventory + Send + Sync>,
{
    fn transaction_commit_dump_changes(&self) -> Result<DeltaMap<DDValue>, String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(&mut writer, "commit dump_changes;")
            .map(|_| DeltaMap::new())
            .map_err(|e| e.to_string())
    }

    fn apply_updates(&self, upds: &mut dyn Iterator<Item = Update<DDValue>>) -> Result<(), String> {
        self.do_record_updates(upds, |i, w, u| Self::record_val_upd(i, w, &u))
    }

    fn query_index(&self, iid: IdxId, key: DDValue) -> Result<BTreeSet<DDValue>, String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(
            &mut writer,
            "query_index {}({});",
            self.inventory.get_index_name(iid).unwrap_or(&"???"),
            key
        )
        .map(|_| BTreeSet::new())
        .map_err(|e| e.to_string())
    }

    fn dump_index(&self, iid: IdxId) -> Result<BTreeSet<DDValue>, String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(
            &mut writer,
            "dump_index {};",
            self.inventory.get_index_name(iid).unwrap_or(&"???")
        )
        .map(|_| BTreeSet::new())
        .map_err(|e| e.to_string())
    }
}

impl<W, I> DDlogDump for CommandRecorder<W, I>
where
    W: Write,
    I: Deref<Target = dyn DDlogInventory + Send + Sync>,
{
    fn dump_table(
        &self,
        rid: RelId,
        _cb: Option<&dyn Fn(&Record, isize) -> bool>,
    ) -> Result<(), String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(
            &mut writer,
            "dump {};",
            self.inventory.get_table_name(rid).unwrap_or(&"???")
        )
        .map_err(|e| e.to_string())
    }

    fn dump_input_snapshot(&self, _w: &mut dyn Write) -> IOResult<()> {
        Ok(())
    }
}

impl<W, I> DDlogProfiling for CommandRecorder<W, I>
where
    W: Write,
    I: Deref<Target = dyn DDlogInventory + Send + Sync>,
{
    fn enable_cpu_profiling(&self, enable: bool) -> Result<(), String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(
            &mut writer,
            "profile cpu {};",
            if enable { "on" } else { "off" }
        )
        .map_err(|e| e.to_string())
    }

    fn enable_timely_profiling(&self, enable: bool) -> Result<(), String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(
            &mut writer,
            "profile timely {};",
            if enable { "on" } else { "off" }
        )
        .map_err(|e| e.to_string())
    }

    fn profile(&self) -> Result<String, String> {
        let mut writer = self.writer.lock().unwrap();
        writeln!(&mut writer, "profile;")
            .map_err(|e| e.to_string())
            .map(|_| "".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyInventory;

    impl DDlogInventory for DummyInventory {
        fn get_table_id(&self, _tname: &str) -> Result<RelId, String> {
            unimplemented!()
        }

        fn get_table_name(&self, _tid: RelId) -> Result<&'static str, String> {
            unimplemented!()
        }

        fn get_index_id(&self, _iname: &str) -> Result<IdxId, String> {
            unimplemented!()
        }

        fn get_index_name(&self, _iid: IdxId) -> Result<&'static str, String> {
            unimplemented!()
        }

        #[cfg(feature = "c_api")]
        fn get_table_cname(&self, _tid: RelId) -> Result<&'static std::ffi::CStr, String> {
            unimplemented!()
        }

        #[cfg(feature = "c_api")]
        fn get_index_cname(&self, _iid: IdxId) -> Result<&'static std::ffi::CStr, String> {
            unimplemented!()
        }
    }

    /// Test recording of "updates" using `record_updates`.
    #[test]
    fn multi_update_recording() {
        fn test(updates: Vec<u64>, expected: &str) {
            let mut buf = Vec::new();
            let iter = updates.iter();

            let recorder = CommandRecorder::new(
                &mut buf,
                Box::new(DummyInventory) as Box<dyn DDlogInventory + Send + Sync>,
            );
            recorder
                .do_record_updates(iter, |_, w, r| write!(w, "update {}", r))
                .unwrap();

            assert_eq!(buf.as_slice(), expected.as_bytes());
        }

        test(Vec::new(), "");
        test(vec![42], "update 42;\n");

        let updates = vec![2, 9, 7, 4, 3, 10];
        let expected = r#"update 2,
update 9,
update 7,
update 4,
update 3,
update 10;
"#;
        test(updates, expected);
    }
}
