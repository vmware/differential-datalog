//! Traits that encapsulate a running DDlog program.

use std::collections::btree_set::BTreeSet;
use std::collections::BTreeMap;
#[cfg(feature = "c_api")]
use std::ffi::CStr;
use std::io;
use std::iter::Iterator;

use crate::ddval::DDValue;
use crate::program::IdxId;
use crate::program::RelId;
use crate::program::Update;
use crate::record::Record;
use crate::record::UpdCmd;
use crate::valmap::DeltaMap;

/// Convert relation and index names to and from numeric id's.
pub trait DDlogInventory {
    /// Convert table name to `RelId`.
    fn get_table_id(&self, tname: &str) -> Result<RelId, String>;

    /// Convert a `RelId` into its symbolic name.
    fn get_table_name(&self, tid: RelId) -> Result<&'static str, String>;

    /// Convert a `RelId` into its symbolic name represented as C string.
    #[cfg(feature = "c_api")]
    fn get_table_cname(&self, tid: RelId) -> Result<&'static CStr, String>;

    /// Convert index name to `IdxId`.
    fn get_index_id(&self, iname: &str) -> Result<IdxId, String>;

    /// Convert a `IdxId` into its symbolic name.
    fn get_index_name(&self, iid: IdxId) -> Result<&'static str, String>;

    /// Convert a `IdxId` into its symbolic name represented as C string.
    #[cfg(feature = "c_api")]
    fn get_index_cname(&self, iid: IdxId) -> Result<&'static CStr, String>;
}

/// Convert to and from values/objects of a DDlog program.
/// FIXME: This trait is redundant and will be removed in the next refactoring.
pub trait DDlogConvert {
    /// Convert an `UpdCmd` into an `Update`.
    fn updcmd2upd(upd_cmd: &UpdCmd) -> Result<Update<DDValue>, String>;
}

/// Interface to DDlog's profiling capabilities.
pub trait DDlogProfiling {
    /// Controls recording of differential operator runtimes.  When enabled,
    /// DDlog records each activation of every operator and prints the
    /// per-operator CPU usage summary in the profile.  When disabled, the
    /// recording stops, but the previously accumulated profile is preserved.
    ///
    /// Recording CPU events can be expensive in large dataflows and is
    /// therefore disabled by default.
    fn enable_cpu_profiling(&self, enable: bool) -> Result<(), String>;

    fn enable_timely_profiling(&self, enable: bool) -> Result<(), String>;

    /// returns DDlog program runtime profile
    fn profile(&self) -> Result<String, String>;
}

/// API to dump DDlog input and output relations.
pub trait DDlogDump {
    fn dump_input_snapshot(&self, w: &mut dyn io::Write) -> io::Result<()>;

    fn dump_table(
        &self,
        table: RelId,
        cb: Option<&dyn Fn(&Record, isize) -> bool>,
    ) -> Result<(), String>;
}
/// A trait capturing the handling of transactions using the dynamically typed
/// representation of DDlog values as `enum Record`.
pub trait DDlogUntyped {
    /// Start a transaction.
    fn transaction_start(&self) -> Result<(), String>;

    /// Commit a transaction previously started using
    /// `transaction_start`, producing a map of deltas.
    fn transaction_commit_dump_changes_untyped(
        &self,
    ) -> Result<BTreeMap<RelId, Vec<(Record, isize)>>, String>;

    /// Commit a transaction previously started using
    /// `transaction_start`.
    fn transaction_commit(&self) -> Result<(), String>;

    /// Roll back a transaction previously started using
    /// `transaction_start`.
    fn transaction_rollback(&self) -> Result<(), String>;

    /// Apply a set of updates.
    fn apply_updates_untyped(&self, upds: &mut dyn Iterator<Item = UpdCmd>) -> Result<(), String>;

    fn clear_relation(&self, table: RelId) -> Result<(), String>;

    /// Query index passing key as a record.  Returns all values associated with the given key in the index.
    fn query_index_untyped(&self, index: IdxId, key: &Record) -> Result<Vec<Record>, String>;

    /// Dump all values in an index.
    fn dump_index_untyped(&self, index: IdxId) -> Result<Vec<Record>, String>;

    /// Stop the program.
    fn stop(&self) -> Result<(), String>;
}

/// Extend `trait DDlog` with methods that offer a strongly typed interface
/// to relations and indexes using `DDValue`.
pub trait DDlogTyped: DDlogUntyped {
    /// Commit a transaction previously started using
    /// `transaction_start`, producing a map of deltas.
    fn transaction_commit_dump_changes(&self) -> Result<DeltaMap<DDValue>, String>;

    /// Apply a set of updates.
    fn apply_updates(&self, upds: &mut dyn Iterator<Item = Update<DDValue>>) -> Result<(), String>;

    /// Query index.  Returns all values associated with the given key in the index.
    fn query_index(&self, index: IdxId, key: DDValue) -> Result<BTreeSet<DDValue>, String>;

    /// Dump all values in an index.
    fn dump_index(&self, index: IdxId) -> Result<BTreeSet<DDValue>, String>;
}
