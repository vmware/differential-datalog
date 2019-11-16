use std::fmt::Debug;
use std::fmt::Display;
use std::io::Result;
use std::io::Write;

use crate::ddlog::DDlogConvert;
use crate::program::RelId;
use crate::program::Update;
use crate::record::RelIdentifier;
use crate::record::UpdCmd;

/// Convert a `RelIdentifier` into its symbolic name.
fn relident2name<C, V>(rel_ident: &RelIdentifier) -> Option<&str>
where
    C: DDlogConvert<Value = V>,
    V: Debug,
{
    match rel_ident {
        RelIdentifier::RelName(rname) => Some(rname.as_ref()),
        RelIdentifier::RelId(id) => C::relid2name(*id),
    }
}

fn record_updates<W, I, U, F>(writer: &mut W, updates: I, record: F) -> Result<()>
where
    W: Write,
    I: Iterator<Item = U>,
    F: Fn(&mut W, &U) -> Result<()>,
{
    // Count the number of elements in `updates`.
    let mut n = 0;

    updates.enumerate().map(Ok(()), |(i, upd)| {
        n += 1;
        if i > 0 {
            writeln!(writer, ",")?;
        };
        record(writer, &upd)?;
        upd
    });

    // Print semicolon if `updates` was not empty.
    if n > 0 {
        writeln!(writer, ";")?;
    }
    Ok(())
}

pub trait RecordReplay: Write {
    fn record_start(&mut self) -> Result<()> {
        writeln!(self, "start;")
    }

    fn record_upd_cmd<C, V>(&mut self, upd: &UpdCmd) -> Result<()>
    where
        C: DDlogConvert<Value = V>,
        V: Debug,
    {
        match upd {
            UpdCmd::Insert(rel, record) => write!(
                self,
                "insert {}[{}]",
                relident2name::<C, _>(rel).unwrap_or(&"???"),
                record
            ),
            UpdCmd::Delete(rel, record) => write!(
                self,
                "delete {}[{}]",
                relident2name::<C, _>(rel).unwrap_or(&"???"),
                record
            ),
            UpdCmd::DeleteKey(rel, record) => write!(
                self,
                "delete_key {} {}",
                relident2name::<C, _>(rel).unwrap_or(&"???"),
                record
            ),
            UpdCmd::Modify(rel, key, mutator) => write!(
                self,
                "modify {} {} <- {}",
                relident2name::<C, _>(rel).unwrap_or(&"???"),
                key,
                mutator
            ),
        }
    }

    fn record_valupdate<C, V>(&mut self, upd: &Update<V>) -> Result<()>
    where
        C: DDlogConvert<Value = V>,
        V: Display + Debug,
    {
        match upd {
            Update::Insert { relid, v } => write!(
                self,
                "insert {}[{}]",
                C::relid2name(*relid).unwrap_or(&"???"),
                v
            ),
            Update::DeleteValue { relid, v } => write!(
                self,
                "delete {}[{}]",
                C::relid2name(*relid).unwrap_or(&"???"),
                v
            ),
            Update::DeleteKey { relid, k } => write!(
                self,
                "delete_key {} {}",
                C::relid2name(*relid).unwrap_or(&"???"),
                k
            ),
            Update::Modify { relid, k, m } => write!(
                self,
                "modify {} {} <- {}",
                C::relid2name(*relid).unwrap_or(&"???"),
                k,
                m
            ),
        }
    }

    /// Record a commit.
    fn record_commit(&mut self, record_changes: bool) -> Result<()> {
        if record_changes {
            writeln!(self, "commit dump_changes;")
        } else {
            writeln!(self, "commit;")
        }
    }

    /// Record a rollback.
    fn record_rollback(&mut self) -> Result<()> {
        writeln!(self, "rollback;")
    }

    /// Record a clear command.
    fn record_clear<C, V>(&mut self, rid: RelId) -> Result<()>
    where
        C: DDlogConvert<Value = V>,
        V: Debug,
    {
        writeln!(self, "clear {};", C::relid2name(rid).unwrap_or(&"???"))
    }

    /// Record a dump command.
    fn record_dump<C, V>(&mut self, rid: RelId) -> Result<()>
    where
        C: DDlogConvert<Value = V>,
        V: Debug,
    {
        writeln!(self, "dump {};", C::relid2name(rid).unwrap_or(&"???"))
    }

    /// Record CPU profiling.
    fn record_cpu_profiling(&mut self, enable: bool) -> Result<()> {
        writeln!(self, "profile cpu {};", if enable { "on" } else { "off" })
    }

    /// Record profiling.
    fn record_profile(&mut self) -> Result<()> {
        writeln!(self, "profile;")
    }
}

impl<W> RecordReplay for W
where
    W: Write,
{
    // The default implementation is just fine.
}
