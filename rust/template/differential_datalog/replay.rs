use std::fmt::Debug;
use std::fmt::Display;
use std::io::Error;
use std::io::Result;
use std::io::Write;
use std::iter::Peekable;

use crate::ddlog::DDlogConvert;
use crate::program::IdxId;
use crate::program::RelId;
use crate::program::Update;
use crate::record::RelIdentifier;
use crate::record::UpdCmd;

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

fn record_updates<'w, W, I, U, F, E>(
    writer: &'w mut W,
    updates: I,
    mut record: F,
    mut error: E,
) -> impl Iterator<Item = U> + 'w
where
    W: Write,
    I: Iterator<Item = U> + 'w,
    F: FnMut(&mut W, &U) -> Result<()> + 'w,
    E: FnMut(Error) + 'w,
{
    Peeking::new(updates).map(move |(upd, last)| {
        let _ = record(writer, &upd)
            .and_then(|_| {
                if !last {
                    writeln!(writer, ",")
                } else {
                    writeln!(writer, ";")
                }
            })
            .map_err(|e| error(e));

        upd
    })
}

/// Record a list of `UpdCmd` objects into the given writable object.
///
/// `error` is a function that is invoked whenever writing out a record
/// failed. Note that such errors do not cause the overall operation to
/// fail.
pub fn record_upd_cmds<'w, C, V, W, I, F>(
    writer: &'w mut W,
    upds: I,
    error: F,
) -> impl Iterator<Item = &'w UpdCmd> + 'w
where
    C: DDlogConvert<Value = V>,
    V: Debug,
    W: Write,
    I: Iterator<Item = &'w UpdCmd> + 'w,
    F: FnMut(Error) + 'w,
{
    record_updates(writer, upds, |w, u| w.record_upd_cmd::<C, _>(&u), error)
}

/// Record a list of `Update` objects into the given writable object.
///
/// `error` is a function that is invoked whenever writing out a record
/// failed. Note that such errors do not cause the overall operation to
/// fail.
pub fn record_val_upds<'w, C, W, I, F, V>(
    writer: &'w mut W,
    upds: I,
    error: F,
) -> impl Iterator<Item = Update<V>> + 'w
where
    C: DDlogConvert<Value = V>,
    V: Display + Debug + 'w,
    W: Write,
    I: Iterator<Item = Update<V>> + 'w,
    F: FnMut(Error) + 'w,
{
    record_updates(writer, upds, |w, u| w.record_val_upd::<C, _>(&u), error)
}

/// A trait for recording various operations into something that can be written
/// to, in order to be able to replay them at a later point in time.
pub trait RecordReplay: Write {
    /// Record a transaction start.
    fn record_start(&mut self) -> Result<()> {
        writeln!(self, "start;")
    }

    fn record_insert<V>(&mut self, name: &str, value: V) -> Result<()>
    where
        V: Display,
    {
        write!(self, "insert {}[{}]", name, value)
    }

    /// Record an `UpdCmd`.
    fn record_upd_cmd<C, V>(&mut self, upd: &UpdCmd) -> Result<()>
    where
        C: DDlogConvert<Value = V>,
        V: Debug,
    {
        match upd {
            UpdCmd::Insert(rel, record) => {
                self.record_insert(relident2name::<C, _>(rel).unwrap_or(&"???"), record)
            }
            UpdCmd::Delete(rel, record) => write!(
                self,
                "delete {}[{}]",
                relident2name::<C, _>(rel).unwrap_or(&"???"),
                record,
            ),
            UpdCmd::DeleteKey(rel, record) => write!(
                self,
                "delete_key {} {}",
                relident2name::<C, _>(rel).unwrap_or(&"???"),
                record,
            ),
            UpdCmd::Modify(rel, key, mutator) => write!(
                self,
                "modify {} {} <- {}",
                relident2name::<C, _>(rel).unwrap_or(&"???"),
                key,
                mutator,
            ),
        }
    }

    /// Record an `Update`.
    fn record_val_upd<C, V>(&mut self, upd: &Update<V>) -> Result<()>
    where
        C: DDlogConvert<Value = V>,
        V: Display + Debug,
    {
        match upd {
            Update::Insert { relid, v } => {
                self.record_insert(C::relid2name(*relid).unwrap_or(&"???"), v)
            }
            Update::DeleteValue { relid, v } => write!(
                self,
                "delete {}[{}]",
                C::relid2name(*relid).unwrap_or(&"???"),
                v,
            ),
            Update::DeleteKey { relid, k } => write!(
                self,
                "delete_key {} {}",
                C::relid2name(*relid).unwrap_or(&"???"),
                k,
            ),
            Update::Modify { relid, k, m } => write!(
                self,
                "modify {} {} <- {}",
                C::relid2name(*relid).unwrap_or(&"???"),
                k,
                m,
            ),
        }
    }

    /// Record a transaction commit.
    fn record_commit(&mut self, record_changes: bool) -> Result<()> {
        if record_changes {
            writeln!(self, "commit dump_changes;")
        } else {
            writeln!(self, "commit;")
        }
    }

    /// Record a transaction rollback.
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

    /// Record a dump_index command.
    fn record_dump_index<C, V>(&mut self, iid: IdxId) -> Result<()>
    where
        C: DDlogConvert<Value = V>,
        V: Debug,
    {
        writeln!(
            self,
            "dump_index {};",
            C::indexid2name(iid).unwrap_or(&"???")
        )
    }

    /// Record a dump_index command.
    fn record_query_index<C, V>(&mut self, iid: IdxId, key: &V) -> Result<()>
    where
        C: DDlogConvert<Value = V>,
        V: Display + Debug,
    {
        writeln!(
            self,
            "query_index {}({});",
            C::indexid2name(iid).unwrap_or(&"???"),
            key
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Test recording of "updates" using `record_updates`.
    #[test]
    fn multi_update_recording() {
        fn test(updates: Vec<u64>, expected: &str) {
            let mut buf = Vec::new();
            let iter = updates.iter();
            let error = |e| panic!("{}", e);

            record_updates(&mut buf, iter, |w, r| write!(w, "update {}", r), error)
                .for_each(|_| ());

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
