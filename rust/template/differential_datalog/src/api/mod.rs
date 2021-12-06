mod c_api;
pub mod update_handler;

#[cfg(feature = "c_api")]
pub use c_api::*;

use crate::flatbuf::FlatbufConverter;
use crate::{
    api::update_handler::{
        ChainedUpdateHandler, DeltaUpdateHandler, IMTUpdateHandler, ThreadUpdateHandler,
        UpdateHandler, ValMapUpdateHandler,
    },
    ddlog::D3logLocalizer,
    ddval::DDValue,
    program::{config::Config, IdxId, Program, RelId, RelationCallback, RunningProgram, Update},
    record::{IntoRecord, Record, UpdCmd},
    replay, AnyDeserialize, CommandRecorder, D3log, D3logLocationId, DDlog, DDlogDump,
    DDlogDynamic, DDlogInventory, DDlogProfiling, DeltaMap,
};
use ddlog_profiler::{CpuProfile, DDlogSourceCode, SizeProfileRecord};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    fmt,
    fs::File,
    io::{self, Write},
    mem,
    os::raw::c_char,
    sync::{Arc, Mutex},
};

type BoxedInventory = Box<dyn DDlogInventory + Send + Sync + 'static>;
type BoxedLocalizer = Box<dyn D3logLocalizer + Send + Sync + 'static>;
type BoxedFlatbufConverter = Box<dyn FlatbufConverter + Send + Sync + 'static>;
type BoxedDeserialize = Box<dyn AnyDeserialize + Send + Sync + 'static>;

// TODO: Move HDDlog into the differential_datalog crate.
pub struct HDDlog {
    pub prog: Mutex<RunningProgram>,
    pub update_handler: Box<dyn IMTUpdateHandler>,
    pub db: Option<Arc<Mutex<DeltaMap<DDValue>>>>,
    pub deltadb: Arc<Mutex<Option<DeltaMap<DDValue>>>>,
    pub print_err: Option<extern "C" fn(msg: *const c_char)>,
    pub inventory: BoxedInventory,
    pub d3log_localizer: BoxedLocalizer,
    pub any_deserialize: Option<BoxedDeserialize>,
    pub flatbuf_converter: BoxedFlatbufConverter,
    /// When set, all commands sent to the program are recorded in
    /// the specified `.dat` file so that they can be replayed later.
    pub command_recorder: Option<CommandRecorder<File, BoxedInventory>>,
}

/* Internals */
impl HDDlog {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        source_code: &'static DDlogSourceCode,
        do_store: bool,
        print_err: Option<extern "C" fn(msg: *const c_char)>,
        init_ddlog: fn(Arc<dyn RelationCallback>) -> Program,
        inventory: BoxedInventory,
        any_deserialize: Option<BoxedDeserialize>,
        d3log_localizer: BoxedLocalizer,
        flatbuf_converter: BoxedFlatbufConverter,
    ) -> Result<(Self, DeltaMap<DDValue>), String> {
        let db: Arc<Mutex<DeltaMap<DDValue>>> = Arc::new(Mutex::new(DeltaMap::new()));
        let db2 = db.clone();

        let deltadb: Arc<Mutex<Option<DeltaMap<_>>>> = Arc::new(Mutex::new(Some(DeltaMap::new())));
        let deltadb2 = deltadb.clone();

        let handler: Box<dyn IMTUpdateHandler> = {
            let handler_generator = move || {
                // Always use delta handler, which costs nothing unless it is
                // actually used
                let delta_handler = DeltaUpdateHandler::new(deltadb2);

                if do_store {
                    let handlers: Vec<Box<dyn UpdateHandler>> = vec![
                        Box::new(delta_handler),
                        Box::new(ValMapUpdateHandler::new(db2)),
                    ];
                    Box::new(ChainedUpdateHandler::new(handlers)) as Box<dyn UpdateHandler>
                } else {
                    Box::new(delta_handler) as Box<dyn UpdateHandler>
                }
            };

            Box::new(ThreadUpdateHandler::new(handler_generator))
        };

        let program = init_ddlog(handler.mt_update_cb());

        // Notify handler about initial transaction
        handler.before_commit();
        let prog = program.run(config, source_code)?;
        handler.after_commit(true);

        // Extract state after initial transaction
        let init_state = deltadb.lock().unwrap().take().unwrap();

        let program = Self {
            prog: Mutex::new(prog),
            update_handler: handler,
            db: Some(db),
            deltadb,
            print_err,
            inventory,
            d3log_localizer,
            any_deserialize,
            flatbuf_converter,
            command_recorder: None,
        };

        Ok((program, init_state))
    }

    pub fn print_err(callback: Option<extern "C" fn(msg: *const c_char)>, msg: &str) {
        match callback {
            None => eprintln!("{}", msg),
            Some(f) => f(CString::new(msg).unwrap().into_raw()),
        }
    }

    pub fn eprintln(&self, msg: &str) {
        Self::print_err(self.print_err, msg)
    }

    pub fn record_commands(&mut self, file: &mut Option<File>) {
        let old_recorder = mem::take(&mut self.command_recorder);

        let mut old_file = old_recorder.map(|recorder| recorder.release_writer());
        mem::swap(file, &mut old_file);

        match old_file {
            None => self.command_recorder = None,
            Some(file) => {
                self.command_recorder = Some(CommandRecorder::new(file, self.inventory.clone()));
            }
        }
    }

    /// Apply a set of updates directly from the flatbuffer
    /// representation
    #[cfg_attr(feature = "flatbuf", doc(hidden))]
    pub fn apply_updates_from_flatbuf(&self, buf: &[u8]) -> Result<(), String> {
        let updates = self.flatbuf_converter.updates_from_buffer(buf)?;
        self.apply_updates(&mut updates.into_iter())
    }

    /// Similar to `query_index`, but extracts query from a flatbuffer.
    #[cfg_attr(feature = "flatbuf", doc(hidden))]
    pub fn query_index_from_flatbuf(&self, buf: &[u8]) -> Result<BTreeSet<DDValue>, String> {
        let (index_id, key) = self.flatbuf_converter.query_index_from_buffer(buf)?;
        self.query_index(index_id, key)
    }

    fn db_dump_table<F>(db: &mut DeltaMap<DDValue>, table: usize, cb: Option<F>)
    where
        F: Fn(&Record, isize) -> bool,
    {
        if let Some(f) = cb {
            for (val, w) in db.get_rel(table) {
                if !f(&val.clone().into_record(), *w) {
                    break;
                }
            }
        };
    }

    fn record_command<T, F>(&self, cmd: F)
    where
        F: FnOnce(
            &CommandRecorder<File, Box<dyn DDlogInventory + Send + Sync>>,
        ) -> Result<T, String>,
    {
        if let Some(ref r) = self.command_recorder {
            let _ = cmd(r).map_err(|e| {
                self.eprintln(&format!(
                    "failed to record invocation in replay file: {}",
                    e
                ));
            });
        }
    }

    pub fn convert_update_command(&self, command: &UpdCmd) -> Result<Update<DDValue>, String> {
        command.to_update(&self.inventory)
    }
}

impl DDlogDump for HDDlog {
    fn dump_input_snapshot(&self, writer: &mut dyn Write) -> io::Result<()> {
        let prog = self.prog.lock().unwrap();

        for (&relation_id, &relation_name) in self.inventory.input_relation_ids() {
            match prog.get_input_relation_data(relation_id) {
                Ok(valset) => {
                    for v in valset.iter() {
                        replay::record_insert(writer, relation_name, v)?;
                        writeln!(writer, ",")?;
                    }
                }

                Err(_) => match prog.get_input_relation_index(relation_id) {
                    Ok(ivalset) => {
                        for v in ivalset.values() {
                            replay::record_insert(writer, relation_name, v)?;
                            writeln!(writer, ",")?;
                        }
                    }

                    Err(_) => match prog.get_input_multiset_data(relation_id) {
                        Ok(ivalmset) => {
                            for (v, weight) in ivalmset.iter() {
                                if *weight >= 0 {
                                    for _ in 0..*weight {
                                        replay::record_insert(writer, relation_name, v)?;
                                        writeln!(writer, ",")?;
                                    }
                                } else {
                                    for _ in 0..(-*weight) {
                                        replay::record_delete(writer, relation_name, v)?;
                                        writeln!(writer, ",")?;
                                    }
                                }
                            }
                        }

                        Err(_) => {
                            // Drop the lock before we panic just to try and avoid poisoning
                            drop(prog);

                            panic!(
                                "Unknown input relation {:?} in dump_input_snapshot",
                                relation_id,
                            );
                        }
                    },
                },
            }
        }

        Ok(())
    }

    fn dump_table(
        &self,
        table: RelId,
        cb: Option<&dyn Fn(&Record, isize) -> bool>,
    ) -> Result<(), String> {
        self.record_command(|r| r.dump_table(table, None));
        if let Some(ref db) = self.db {
            HDDlog::db_dump_table(&mut db.lock().unwrap(), table, cb);
            Ok(())
        } else {
            Err(
                "cannot dump table: ddlog_run() was invoked with do_store flag set to false"
                    .to_string(),
            )
        }
    }
}

impl DDlogProfiling for HDDlog {
    fn enable_cpu_profiling(&self, enable: bool) -> Result<(), String> {
        self.record_command(|r| r.enable_cpu_profiling(enable));
        self.prog.lock().unwrap().enable_cpu_profiling(enable);
        Ok(())
    }

    fn enable_change_profiling(&self, enable: bool) -> Result<(), String> {
        self.record_command(|r| r.enable_change_profiling(enable));
        self.prog.lock().unwrap().enable_change_profiling(enable);
        Ok(())
    }

    fn enable_timely_profiling(&self, enable: bool) -> Result<(), String> {
        self.record_command(|r| r.enable_timely_profiling(enable));
        self.prog.lock().unwrap().enable_timely_profiling(enable);
        Ok(())
    }

    fn arrangement_size_profile(&self) -> Result<Vec<SizeProfileRecord>, String> {
        self.record_command(|r| r.arrangement_size_profile());
        let rprog = self.prog.lock().unwrap();
        if let Some(profile) = &rprog.profile {
            Ok(profile.lock().unwrap().arrangement_size_profile())
        } else {
            Err("DDlog instance was created with self-profiler disabled".to_string())
        }
    }

    fn peak_arrangement_size_profile(&self) -> Result<Vec<SizeProfileRecord>, String> {
        self.record_command(|r| r.peak_arrangement_size_profile());
        let rprog = self.prog.lock().unwrap();
        if let Some(profile) = &rprog.profile {
            Ok(profile.lock().unwrap().peak_arrangement_size_profile())
        } else {
            Err("DDlog instance was created with self-profiler disabled".to_string())
        }
    }

    fn change_profile(&self) -> Result<Option<Vec<SizeProfileRecord>>, String> {
        self.record_command(|r| r.change_profile());
        let rprog = self.prog.lock().unwrap();
        if let Some(profile) = &rprog.profile {
            Ok(profile.lock().unwrap().change_profile())
        } else {
            Err("DDlog instance was created with self-profiler disabled".to_string())
        }
    }

    fn cpu_profile(&self) -> Result<Option<CpuProfile>, String> {
        self.record_command(|r| r.cpu_profile());
        let rprog = self.prog.lock().unwrap();
        if let Some(profile) = &rprog.profile {
            Ok(profile.lock().unwrap().cpu_profile())
        } else {
            Err("DDlog instance was created with self-profiler disabled".to_string())
        }
    }

    fn dump_profile(&self, label: Option<&str>) -> Result<String, String> {
        self.record_command(|r| r.dump_profile(label));
        let rprog = self.prog.lock().unwrap();
        if let Some(profile) = &rprog.profile {
            profile.lock().unwrap().dump(label)
        } else {
            Err("DDlog instance was created with self-profiler disabled".to_string())
        }
    }
}

impl DDlogDynamic for HDDlog {
    fn transaction_start(&self) -> Result<(), String> {
        self.record_command(|r| r.transaction_start());
        self.prog.lock().unwrap().transaction_start()
    }

    fn transaction_commit(&self) -> Result<(), String> {
        self.record_command(|r| r.transaction_commit());
        self.update_handler.before_commit();

        match self.prog.lock().unwrap().transaction_commit() {
            Ok(()) => {
                self.update_handler.after_commit(true);
                Ok(())
            }

            Err(e) => {
                self.update_handler.after_commit(false);
                Err(e)
            }
        }
    }

    fn transaction_commit_dump_changes_dynamic(
        &self,
    ) -> Result<BTreeMap<RelId, Vec<(Record, isize)>>, String> {
        self.record_command(|r| r.transaction_commit_dump_changes_dynamic());
        Ok(self
            .do_transaction_commit_dump_changes(false)?
            .into_iter()
            .map(|(relid, delta_typed)| {
                let delta_dynamic: Vec<(Record, isize)> = delta_typed
                    .into_iter()
                    .map(|(v, w)| (v.into_record(), w))
                    .collect();
                (relid, delta_dynamic)
            })
            .collect())
    }

    fn transaction_rollback(&self) -> Result<(), String> {
        self.record_command(|r| r.transaction_rollback());
        self.prog.lock().unwrap().transaction_rollback()
    }

    fn clear_relation(&self, table: RelId) -> Result<(), String> {
        self.record_command(|r| r.clear_relation(table));
        self.prog.lock().unwrap().clear_relation(table)
    }

    fn apply_updates_dynamic(&self, upds: &mut dyn Iterator<Item = UpdCmd>) -> Result<(), String> {
        let mut conversion_err = false;
        let mut msg: Option<String> = None;

        // Iterate through all updates, but only feed them to `apply_updates` until we reach
        // the first invalid command.
        // XXX: We must iterate till the end of `upds`, as `ddlog_apply_updates` relies on this to
        // deallocate all commands.
        let convert = |update: UpdCmd| {
            if conversion_err {
                None
            } else {
                match self.convert_update_command(&update) {
                    Ok(u) => Some(u),
                    Err(e) => {
                        conversion_err = true;
                        msg = Some(format!("invalid command {:?}: {}", update, e));
                        None
                    }
                }
            }
        };

        let res = if self.command_recorder.is_some() {
            let update_vec: Vec<_> = upds.collect();
            self.record_command(|r| r.apply_updates_dynamic(&mut update_vec.iter().cloned()));

            self.do_apply_updates(
                &mut update_vec.into_iter().flat_map(convert)
                    as &mut dyn Iterator<Item = Update<DDValue>>,
                false,
            )
        } else {
            self.do_apply_updates(
                &mut upds.flat_map(convert) as &mut dyn Iterator<Item = Update<DDValue>>,
                false,
            )
        };

        match msg {
            Some(e) => Err(e),
            None => res,
        }
    }

    fn query_index_dynamic(&self, index: IdxId, key: &Record) -> Result<Vec<Record>, String> {
        self.record_command(|r| r.query_index_dynamic(index, key));

        let key = self.inventory.index_from_record(index, key)?;
        let results = self
            .do_query_index(index, key, false)?
            .into_iter()
            .map(DDValue::into_record)
            .collect();

        Ok(results)
    }

    fn dump_index_dynamic(&self, index: IdxId) -> Result<Vec<Record>, String> {
        self.record_command(|r| r.dump_index_dynamic(index));
        Ok(self
            .do_dump_index(index, false)?
            .into_iter()
            .map(|v| v.into_record())
            .collect())
    }

    fn stop(&self) -> Result<(), String> {
        self.prog.lock().unwrap().stop()
    }
}

// Implement methods from the `DDlog` trait with an extra flag to control command recording.  The
// flag is set to `true` when invoked from `impl DDlog` and `false` when invoked from `impl
// DDlogDynamic`, since the latter does its own recording.
impl HDDlog {
    fn do_transaction_commit_dump_changes(
        &self,
        record: bool,
    ) -> Result<DeltaMap<DDValue>, String> {
        if record {
            self.record_command(|r| r.transaction_commit_dump_changes());
        }
        *self.deltadb.lock().unwrap() = Some(DeltaMap::new());

        self.update_handler.before_commit();
        match self.prog.lock().unwrap().transaction_commit() {
            Ok(()) => {
                self.update_handler.after_commit(true);
                let mut delta = self.deltadb.lock().unwrap();
                Ok(delta.take().unwrap())
            }

            Err(e) => {
                self.update_handler.after_commit(false);
                Err(e)
            }
        }
    }

    fn do_apply_updates(
        &self,
        upds: &mut dyn Iterator<Item = Update<DDValue>>,
        record: bool,
    ) -> Result<(), String> {
        // Make sure that the updates being inserted have the correct value types for their
        // relation
        let inspect_update = |update: &Update<DDValue>| {
            let relation_type = self
                .inventory
                .relation_type_id(update.relid())
                .ok_or_else(|| format!("unknown relation id {}", update.relid()))?;

            if let Some(value) = update.get_value() {
                if relation_type != value.type_id() {
                    return Err(format!("attempted to insert the incorrect type {:?} into relation {:?} whose value type is {:?}", value.type_id(), relation_type, relation_type));
                }
            }

            Ok(())
        };

        if record && self.command_recorder.is_some() {
            let update_vec: Vec<_> = upds.collect();
            self.record_command(|r| r.apply_updates(&mut update_vec.iter().cloned()));

            self.prog
                .lock()
                .unwrap()
                .apply_updates(&mut update_vec.into_iter(), inspect_update)
        } else {
            self.prog
                .lock()
                .unwrap()
                .apply_updates(upds, inspect_update)
        }
    }

    fn do_query_index(
        &self,
        index: IdxId,
        key: DDValue,
        record: bool,
    ) -> Result<BTreeSet<DDValue>, String> {
        if record {
            self.record_command(|r| r.query_index(index, key.clone()));
        }
        let arrangement_id = self
            .inventory
            .index_to_arrangement_id(index)
            .ok_or_else(|| format!("unknown index {}", index))?;

        self.prog
            .lock()
            .unwrap()
            .query_arrangement(arrangement_id, key)
    }

    fn do_dump_index(&self, index: IdxId, record: bool) -> Result<BTreeSet<DDValue>, String> {
        if record {
            self.record_command(|r| r.dump_index(index));
        }
        let arrangement_id = self
            .inventory
            .index_to_arrangement_id(index)
            .ok_or_else(|| format!("unknown index {}", index))?;

        self.prog.lock().unwrap().dump_arrangement(arrangement_id)
    }
}

impl DDlog for HDDlog {
    fn transaction_commit_dump_changes(&self) -> Result<DeltaMap<DDValue>, String> {
        self.do_transaction_commit_dump_changes(true)
    }

    fn apply_updates(&self, upds: &mut dyn Iterator<Item = Update<DDValue>>) -> Result<(), String> {
        self.do_apply_updates(upds, true)
    }

    fn query_index(&self, index: IdxId, key: DDValue) -> Result<BTreeSet<DDValue>, String> {
        self.do_query_index(index, key, true)
    }

    fn dump_index(&self, index: IdxId) -> Result<BTreeSet<DDValue>, String> {
        self.do_dump_index(index, true)
    }
}

impl D3log for HDDlog {
    fn d3log_localize_val(
        &self,
        relation_id: RelId,
        value: DDValue,
    ) -> Result<(Option<D3logLocationId>, RelId, DDValue), DDValue> {
        self.d3log_localizer.localize_value(relation_id, value)
    }
}

impl fmt::Debug for HDDlog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HDDlog")
            .field("prog", &self.prog)
            .field("update_handler", &(&*self.update_handler as *const _))
            .field("db", &self.db)
            .field("deltadb", &self.deltadb)
            .field("print_err", &self.print_err)
            .field("inventory", &(&*self.inventory as *const _))
            .field("d3log_localizer", &(&*self.d3log_localizer as *const _))
            .field("command_recorder", &self.command_recorder)
            .finish()
    }
}
