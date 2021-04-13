use crate::{
    ddval::DDValue,
    profile::{get_prof_context, with_prof_context, ProfMsg},
    program::{
        arrange::{ArrangedCollection, Arrangements},
        ArrId, Dep, Msg, ProgNode, Program, Reply, Update, TS,
    },
    program::{RelId, Weight},
    variable::Variable,
};
use crossbeam_channel::{Receiver, Sender};
use differential_dataflow::{
    input::{Input, InputSession},
    logging::DifferentialEvent,
    operators::{
        arrange::{ArrangeBySelf, TraceAgent},
        iterate::Variable as DDVariable,
        Consolidate, ThresholdTotal,
    },
    trace::{
        implementations::{ord::OrdValBatch, spine_fueled::Spine},
        BatchReader, Cursor, TraceReader,
    },
    Collection,
};
use dogsdogsdogs::operators::lookup_map;
use fnv::{FnvBuildHasher, FnvHashMap};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    mem,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use timely::{
    communication::Allocator,
    dataflow::{operators::probe::Handle as ProbeHandle, scopes::Child, Scope},
    logging::TimelyEvent,
    progress::frontier::AntichainRef,
    worker::Worker,
};

// Handles to objects involved in managing the progress of the dataflow.
struct SessionData {
    // Input sessions for program relations.
    sessions: FnvHashMap<RelId, InputSession<TS, DDValue, Weight>>,
    // Input session for the special `Enabled` relation (see detailed comment in
    // `session_dataflow()`).
    enabled_session: InputSession<TS, (), Weight>,
    // Traces for arrangements used in indexes.
    traces: BTreeMap<
        ArrId,
        TraceAgent<
            Spine<DDValue, DDValue, u32, i32, Rc<OrdValBatch<DDValue, DDValue, u32, i32, u32>>>,
        >,
    >,
}

/// A DDlog timely worker
pub struct DDlogWorker<'a> {
    /// The timely worker instance
    worker: &'a mut Worker<Allocator>,
    /// The program this worker is executing
    program: Arc<Program>,
    /// Information on which metrics are enabled and a
    /// channel for sending profiling data
    profiling: ProfilingData,
    /// The current worker's receiver for receiving messages
    request_receiver: Receiver<Msg>,
    /// The current worker's sender for sending messages
    reply_sender: Sender<Reply>,
}

impl<'a> DDlogWorker<'a> {
    /// Create a new ddlog timely worker
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        worker: &'a mut Worker<Allocator>,
        program: Arc<Program>,
        profiling: ProfilingData,
        request_receivers: Arc<[Receiver<Msg>]>,
        reply_senders: Arc<[Sender<Reply>]>,
    ) -> Self {
        let worker_index = worker.index();

        Self {
            worker,
            program,
            profiling,
            request_receiver: request_receivers[worker_index].clone(),
            reply_sender: reply_senders[worker_index].clone(),
        }
    }

    /// Returns whether or not the current worker is the leader, worker 0
    pub fn is_leader(&self) -> bool {
        self.worker_index() == 0
    }

    /// Get the index of the current worker
    pub fn worker_index(&self) -> usize {
        self.worker.index()
    }

    pub fn run(mut self) -> Result<(), String> {
        // Initialize profiling
        self.init_profiling();

        let probe = ProbeHandle::new();
        let mut session_data = self.session_dataflow(probe.clone())?;

        self.ingest_initial_data(&mut session_data, &probe)?;

        // Close session handles for non-input sessions.
        session_data.sessions = session_data
            .sessions
            .drain()
            .filter(|&(relid, _)| self.program.get_relation(relid).input)
            .collect();

        // Keeps track of the last timestamp we've seen in a `Flush` message.
        let mut timestamp = 1;

        'worker_loop: loop {
            // Non-blocking receive, so that we can do some garbage collecting
            // when there is no real work to do.
            let messages: Vec<_> = self.request_receiver.try_iter().fuse().collect();
            for message in messages {
                match message {
                    // Update inputs with the given updates at the given timestamp
                    Msg::Update { updates, timestamp } => {
                        self.batch_update(&mut session_data.sessions, updates, timestamp)?
                    }

                    // The `Flush` message gives us the timestamp to advance to, so advance there
                    // before flushing & compacting all previous timestamp's traces
                    Msg::Flush { advance_to } => {
                        self.advance(&mut session_data, advance_to);
                        self.flush(&mut session_data, &probe);
                        timestamp = advance_to;

                        self.reply_sender
                            .send(Reply::FlushAck)
                            .map_err(|e| format!("failed to send ACK: {}", e))?;
                    }

                    // Handle queries
                    Msg::Query(arrid, key) => {
                        self.handle_query(&mut session_data.traces, arrid, key)?
                    }

                    // On either the stop message or a channel disconnection we can shut down
                    // the computation.
                    Msg::Stop => {
                        self.disable(&mut session_data, timestamp, &probe);
                        // TODO: Log worker #n disconnection
                        break 'worker_loop;
                    }
                }
            }

            // After each batch of commands received we step, if there's no timely work to be
            // done, our thread will be parked until it's re-awoken by a command.
            self.worker.step_or_park(None);
        }

        Ok(())
    }

    /// Applies updates to the current worker's input sessions
    fn batch_update(
        &self,
        sessions: &mut FnvHashMap<RelId, InputSession<TS, DDValue, Weight>>,
        updates: Vec<Update<DDValue>>,
        timestamp: TS,
    ) -> Result<(), String> {
        for update in updates {
            match update {
                Update::Insert { relid, v } => {
                    sessions
                        .get_mut(&relid)
                        .ok_or_else(|| format!("no session found for relation ID {}", relid))?
                        .update_at(v, timestamp, 1);
                }

                Update::DeleteValue { relid, v } => {
                    sessions
                        .get_mut(&relid)
                        .ok_or_else(|| format!("no session found for relation ID {}", relid))?
                        .update_at(v, timestamp, -1);
                }

                Update::InsertOrUpdate { .. } => {
                    return Err("InsertOrUpdate command received by worker thread".to_string());
                }

                Update::DeleteKey { .. } => {
                    // workers don't know about keys
                    return Err("DeleteKey command received by worker thread".to_string());
                }

                Update::Modify { .. } => {
                    return Err("Modify command received by worker thread".to_string());
                }
            }
        }

        Ok(())
    }

    /// Feeds the startup data into the computation
    fn ingest_initial_data(
        &mut self,
        session_data: &mut SessionData,
        probe: &ProbeHandle<TS>,
    ) -> Result<(), String> {
        // We immediately advance to a timestamp of 1, since timestamp 0 will contain
        // our initial data
        let timestamp = 1;

        // Only the leader introduces data into the input sessions
        if self.is_leader() {
            for (relid, v) in self.program.init_data.iter() {
                session_data
                    .sessions
                    .get_mut(relid)
                    .ok_or_else(|| format!("no session found for relation ID {}", relid))?
                    .update_at(v.clone(), timestamp - 1, 1);
            }
            // Insert a record in the Enabled relation.
            session_data.enabled_session.update_at((), timestamp - 1, 1);
        }

        // All workers advance to timestamp 1 and flush their inputs
        self.advance(session_data, timestamp);
        self.flush(session_data, probe);

        self.reply_sender
            .send(Reply::FlushAck)
            .map_err(|e| format!("failed to send ACK: {}", e))?;

        Ok(())
    }

    /// Empty the Enabled relation to help the dataflow terminate.
    fn disable(&mut self, session_data: &mut SessionData, timestamp: TS, probe: &ProbeHandle<TS>) {
        if self.is_leader() {
            // Delete the sole record from the Enabled relation.
            session_data.enabled_session.update_at((), timestamp, -1);
        }

        self.advance(session_data, timestamp + 1);
        self.flush(session_data, probe);
    }

    /// Advance the epoch on all input sessions
    fn advance(&self, session_data: &mut SessionData, timestamp: TS) {
        for session_input in session_data.sessions.values_mut() {
            session_input.advance_to(timestamp);
        }
        session_data.enabled_session.advance_to(timestamp);

        for trace in session_data.traces.values_mut() {
            let e = [timestamp];
            let ac = AntichainRef::new(&e);
            trace.set_physical_compaction(ac);
            trace.set_logical_compaction(ac);
        }
    }

    /// Propagate all changes through the pipeline
    fn flush(&mut self, session_data: &mut SessionData, probe: &ProbeHandle<TS>) {
        for relation_input in session_data.sessions.values_mut() {
            relation_input.flush();
        }
        session_data.enabled_session.flush();

        if let Some(session) = session_data.sessions.values_mut().next() {
            while probe.less_than(session.time()) {
                self.worker.step_or_park(None);
            }
        }
    }

    /// Handle a query
    fn handle_query<Trace>(
        &self,
        traces: &mut BTreeMap<ArrId, Trace>,
        arrid: ArrId,
        key: Option<DDValue>,
    ) -> Result<(), String>
    where
        Trace: TraceReader<Key = DDValue, Val = DDValue, Time = TS, R = Weight>,
        <Trace as TraceReader>::Batch: BatchReader<DDValue, DDValue, TS, Weight>,
        <Trace as TraceReader>::Cursor: Cursor<DDValue, DDValue, TS, Weight>,
    {
        let trace = match traces.get_mut(&arrid) {
            Some(trace) => trace,
            None => {
                self.reply_sender
                    .send(Reply::QueryRes(None))
                    .map_err(|e| format!("handle_query: failed to send error response: {}", e))?;

                return Ok(());
            }
        };

        let (mut cursor, storage) = trace.cursor();
        // for ((k, v), diffs) in cursor.to_vec(&storage).iter() {
        //     println!("{:?}:{:?}: {:?}", *k, *v, diffs);
        // }

        /* XXX: is this necessary? */
        cursor.rewind_keys(&storage);
        cursor.rewind_vals(&storage);

        let values = match key {
            Some(k) => {
                cursor.seek_key(&storage, &k);
                if !cursor.key_valid(&storage) {
                    BTreeSet::new()
                } else {
                    let mut values = BTreeSet::new();
                    while cursor.val_valid(&storage) && *cursor.key(&storage) == k {
                        let mut weight = 0;
                        cursor.map_times(&storage, |_, &diff| weight += diff);

                        //assert!(weight >= 0);
                        // FIXME: this will add the value to the set even if `weight < 0`,
                        // i.e., positive and negative weights are treated the same way.
                        // A negative wait should only be possible if there are values with
                        // negative weights in one of the input multisets.
                        if weight != 0 {
                            values.insert(cursor.val(&storage).clone());
                        }

                        cursor.step_val(&storage);
                    }

                    values
                }
            }

            None => {
                let mut values = BTreeSet::new();
                while cursor.key_valid(&storage) {
                    while cursor.val_valid(&storage) {
                        let mut weight = 0;
                        cursor.map_times(&storage, |_, &diff| weight += diff);

                        //assert!(weight >= 0);
                        if weight != 0 {
                            values.insert(cursor.val(&storage).clone());
                        }

                        cursor.step_val(&storage);
                    }

                    cursor.step_key(&storage);
                }

                values
            }
        };

        self.reply_sender
            .send(Reply::QueryRes(Some(values)))
            .map_err(|e| format!("handle_query: failed to send query response: {}", e))?;

        Ok(())
    }

    /// Initialize timely and differential profiling logging hooks
    fn init_profiling(&self) {
        let profiling = self.profiling.clone();
        self.worker
            .log_register()
            .insert::<TimelyEvent, _>("timely", move |_time, data| {
                let profile_cpu = profiling.is_cpu_enabled();
                let profile_timely = profiling.is_timely_enabled();

                // Filter out events we don't care about to avoid the overhead of sending
                // the event around just to drop it eventually.
                let filtered: Vec<((Duration, usize, TimelyEvent), Option<String>)> = data
                    .drain(..)
                    .filter(|event| {
                        match event.2 {
                            // Always send Operates events as they're used for always-on memory profiling.
                            TimelyEvent::Operates(_) => true,

                            // Send scheduling events if profiling is enabled
                            TimelyEvent::Schedule(_) => profile_cpu || profile_timely,

                            // Send timely events if timely profiling is enabled
                            TimelyEvent::GuardedMessage(_)
                            | TimelyEvent::Messages(_)
                            | TimelyEvent::Park(_)
                            | TimelyEvent::PushProgress(_) => profile_timely,

                            _ => false,
                        }
                    })
                    .map(|(d, s, e)| match e {
                        // Only Operate events care about the context string.
                        TimelyEvent::Operates(_) => ((d, s, e), Some(get_prof_context())),
                        _ => ((d, s, e), None),
                    })
                    .collect();

                // If there are any profiling events, record them
                if !filtered.is_empty() {
                    profiling.record(ProfMsg::TimelyMessage(
                        filtered,
                        profile_cpu,
                        profile_timely,
                    ));
                }
            });

        let profiling = self.profiling.clone();
        self.worker.log_register().insert::<DifferentialEvent, _>(
            "differential/arrange",
            move |_time, data| {
                // If there are events, send them through the profiling channel
                if !data.is_empty() {
                    profiling.record(ProfMsg::DifferentialMessage(mem::take(data)));
                }
            },
        );
    }

    fn session_dataflow(&mut self, mut probe: ProbeHandle<TS>) -> Result<SessionData, String> {
        let program = self.program.clone();

        self.worker.dataflow::<TS, _, _>(|outer: &mut Child<Worker<Allocator>, TS>| -> Result<_, String> {
            let mut sessions: FnvHashMap<RelId, InputSession<TS, DDValue, Weight>> = FnvHashMap::default();
            let mut collections: FnvHashMap<RelId, Collection<Child<Worker<Allocator>, TS>, DDValue, Weight>> =
                    HashMap::with_capacity_and_hasher(program.nodes.len(), FnvBuildHasher::default());
            let mut arrangements = FnvHashMap::default();

            // Create an `Enabled` relation used to enforce the dataflow termination in the
            // presence of delayed relations.  A delayed relation can potentially generate an
            // infinite sequence of outputs for all future timestamps, preventing the
            // differential dataflow from terminating.  DD can only terminate cleanly when there
            // is no data in flight, and will keep spinning forever if new records keep getting
            // injected in the dataflow.  To prevent this scenario, we join all delayed relations
            // with `Enabled`.  `Enabled` contains a single record (an empty tuple) as long as
            // the program is running.  We retract this record on shutdown, hopefully enforcing
            // quiescing the dataflow.
            // Note: this trick won't be needed once DD supports proactive termination:
            // https://github.com/TimelyDataflow/timely-dataflow/issues/306
            let (enabled_session, enabled_collection) = outer.new_collection::<(),Weight>();
            let enabled_arrangement = enabled_collection.arrange_by_self();

            // Create variables for delayed relations.  We will be able to refer to these variables
            // inside rules and assign them once all rules have been evaluated.
            let delayed_vars: FnvHashMap<RelId, (RelId, DDVariable<Child<Worker<Allocator>, TS>, DDValue, Weight>, Collection<Child<Worker<Allocator>, TS>, DDValue, Weight>)> = program.delayed_rels.iter().map(|drel| {
                let v = DDVariable::new(outer, drel.delay);

                let vcol = with_prof_context(
                            &format!("join {} with 'Enabled' relation", drel.id),
                            || lookup_map(
                                &v,
                                enabled_arrangement.clone(),
                                |_: &DDValue, key| *key = (),
                                move |x, w, _, _| (x.clone(), *w),
                                (),
                                (),
                                (),
                            )
                            );
                (drel.id, (drel.rel_id, v, vcol))
            }).collect();

            for (nodeid, node) in program.nodes.iter().enumerate() {
                match node {
                    ProgNode::Rel{rel} => {
                        // Relation may already be in the map if it was created by an `Apply` node
                        let mut collection = collections
                            .remove(&rel.id)
                            .unwrap_or_else(|| {
                                let (session, collection) = outer.new_collection::<DDValue, Weight>();
                                sessions.insert(rel.id, session);

                                collection
                            });

                        // apply rules
                        let rule_collections = rel
                            .rules
                            .iter()
                            .map(|rule| {
                                program.mk_rule(
                                    rule,
                                    |rid| collections.get(&rid).or_else(|| delayed_vars.get(&rid).map(|v| &v.2)),
                                    Arrangements {
                                        arrangements1: &arrangements,
                                        arrangements2: &FnvHashMap::default(),
                                    },
                                    true
                                )
                            });

                        collection = with_prof_context(
                            &format!("concatenate rules for {}", rel.name),
                            || collection.concatenate(rule_collections),
                        );

                        // don't distinct input collections, as this is already done by the set_update logic
                        if !rel.input && rel.distinct {
                            collection = with_prof_context(
                                &format!("{}.threshold_total", rel.name),
                                || collection.threshold_total(|_, c| if *c == 0 { 0 } else { 1 }),
                            );
                        }

                        // create arrangements
                        for (i,arr) in rel.arrangements.iter().enumerate() {
                            with_prof_context(
                                arr.name(),
                                || arrangements.insert(
                                    (rel.id, i),
                                    arr.build_arrangement_root(&collection),
                                ),
                            );
                        }

                        collections.insert(rel.id, collection);
                    },
                    &ProgNode::Apply { tfun } => {
                        tfun()(&mut collections);
                    },
                    ProgNode::SCC { rels } => {
                        // Preallocate the memory required to store the new relations
                        sessions.reserve(rels.len());
                        collections.reserve(rels.len());

                        // create collections; add them to map; we will overwrite them with
                        // updated collections returned from the inner scope.
                        for r in rels.iter() {
                            let (session, collection) = outer.new_collection::<DDValue,Weight>();
                            //assert!(!r.rel.input, "input relation in nested scope: {}", r.rel.name);
                            if r.rel.input {
                                return Err(format!("input relation in nested scope: {}", r.rel.name));
                            }

                            sessions.insert(r.rel.id, session);
                            collections.insert(r.rel.id, collection);
                        }

                        // create a nested scope for mutually recursive relations
                        let new_collections = outer.scoped("recursive component", |inner| -> Result<_, String> {
                            // create variables for relations defined in the SCC.
                            let mut vars = HashMap::with_capacity_and_hasher(rels.len(), FnvBuildHasher::default());
                            // arrangements created inside the nested scope
                            let mut local_arrangements = FnvHashMap::default();
                            // arrangements entered from global scope
                            let mut inner_arrangements = FnvHashMap::default();

                            for r in rels.iter() {
                                let var = Variable::from(
                                    &collections
                                        .get(&r.rel.id)
                                        .ok_or_else(|| format!("failed to find collection with relation ID {}", r.rel.id))?
                                        .enter(inner),
                                    r.distinct,
                                    &r.rel.name,
                                );

                                vars.insert(r.rel.id, var);
                            }

                            // create arrangements
                            for rel in rels {
                                for (i, arr) in rel.rel.arrangements.iter().enumerate() {
                                    // check if arrangement is actually used inside this node
                                    if program.arrangement_used_by_nodes((rel.rel.id, i)).any(|n| n == nodeid) {
                                        with_prof_context(
                                            &format!("local {}", arr.name()),
                                            || local_arrangements.insert(
                                                (rel.rel.id, i),
                                                arr.build_arrangement(&*vars.get(&rel.rel.id)?),
                                            ),
                                        );
                                    }
                                }
                            }

                            let dependencies = Program::dependencies(rels.iter().map(|relation| &relation.rel));

                            // collections entered from global scope
                            let mut inner_collections = HashMap::with_capacity_and_hasher(dependencies.len(), FnvBuildHasher::default());

                            for dep in dependencies {
                                match dep {
                                    Dep::Rel(relid) => {
                                        assert!(!vars.contains_key(&relid));
                                        let collection = collections
                                            .get(&relid)
                                            .ok_or_else(|| format!("failed to find collection with relation ID {}", relid))?
                                            .enter(inner);

                                        inner_collections.insert(relid, collection);
                                    },
                                    Dep::Arr(arrid) => {
                                        let arrangement = arrangements
                                            .get(&arrid)
                                            .ok_or_else(|| format!("Arr: unknown arrangement {:?}", arrid))?
                                            .enter(inner);

                                        inner_arrangements.insert(arrid, arrangement);
                                    }
                                }
                            }

                            // apply rules to variables
                            for rel in rels {
                                for rule in &rel.rel.rules {
                                    let c = program.mk_rule(
                                        rule,
                                        |rid| {
                                            vars
                                                .get(&rid)
                                                .map(|v| &(**v))
                                                .or_else(|| inner_collections.get(&rid))
                                        },
                                        Arrangements {
                                            arrangements1: &local_arrangements,
                                            arrangements2: &inner_arrangements,
                                        },
                                        false
                                    );

                                    vars
                                        .get_mut(&rel.rel.id)
                                        .ok_or_else(|| format!("no variable found for relation ID {}", rel.rel.id))?
                                        .add(&c);
                                }
                            }

                            // bring new relations back to the outer scope
                            let mut new_collections = HashMap::with_capacity_and_hasher(rels.len(), FnvBuildHasher::default());
                            for rel in rels {
                                let var = vars
                                    .get(&rel.rel.id)
                                    .ok_or_else(|| format!("no variable found for relation ID {}", rel.rel.id))?;

                                let mut collection = var.leave();
                                // var.distinct() will be called automatically by var.drop() if var has `distinct` flag set
                                if rel.rel.distinct && !rel.distinct {
                                    collection = with_prof_context(
                                        &format!("{}.distinct_total", rel.rel.name),
                                        || collection.threshold_total(|_,c| if *c == 0 { 0 } else { 1 }),
                                    );
                                }

                                new_collections.insert(rel.rel.id, collection);
                            }

                            Ok(new_collections)
                        })?;

                        // add new collections to the map
                        collections.extend(new_collections);

                        // create arrangements
                        for rel in rels {
                            for (i, arr) in rel.rel.arrangements.iter().enumerate() {
                                // only if the arrangement is used outside of this node
                                if arr.queryable() || program.arrangement_used_by_nodes((rel.rel.id, i)).any(|n| n != nodeid) {
                                    with_prof_context(
                                        &format!("global {}", arr.name()),
                                        || -> Result<_, String> {
                                            let collection = collections
                                                .get(&rel.rel.id)
                                                .ok_or_else(|| format!("no collection found for relation ID {}", rel.rel.id))?;

                                            Ok(arrangements.insert((rel.rel.id, i), arr.build_arrangement(collection)))
                                        }
                                    )?;
                                }
                            }
                        }
                    }
                }
            };

            for (id, (relid, v, _)) in delayed_vars.into_iter() {
                v.set(&collections.get(&relid).ok_or_else(|| format!("delayed variable {} refers to unknown base relation {}", id, relid))?.consolidate());
            };

            for (relid, collection) in collections {
                // notify client about changes
                if let Some(relation_callback) = &program.get_relation(relid).change_cb {
                    let relation_callback = relation_callback.clone();

                    let consolidated = with_prof_context(
                        &format!("consolidate {}", relid),
                        || collection.consolidate(),
                    );

                    let inspected = with_prof_context(
                        &format!("inspect {}", relid),
                        || consolidated.inspect(move |x| {
                            // assert!(x.2 == 1 || x.2 == -1, "x: {:?}", x);
                            (relation_callback)(relid, &x.0, x.2)
                        }),
                    );

                    with_prof_context(
                        &format!("probe {}", relid),
                        || inspected.probe_with(&mut probe),
                    );
                }
            }

            // Attach probes to index arrangements, so we know when all updates
            // for a given epoch have been added to the arrangement, and return
            // arrangement trace.
            let mut traces: BTreeMap<ArrId, _> = BTreeMap::new();
            for ((relid, arrid), arr) in arrangements.into_iter() {
                if let ArrangedCollection::Map(arranged) = arr {
                    if program.get_relation(relid).arrangements[arrid].queryable() {
                        arranged.as_collection(|k,_| k.clone()).probe_with(&mut probe);
                        traces.insert((relid, arrid), arranged.trace.clone());
                    }
                }
            }

            Ok(SessionData{
                sessions,
                enabled_session,
                traces
            })
        })
    }
}

#[derive(Clone)]
pub struct ProfilingData {
    /// Whether CPU profiling is enabled
    cpu_enabled: Arc<AtomicBool>,
    /// Whether timely profiling is enabled
    timely_enabled: Arc<AtomicBool>,
    /// The channel used to send profiling data to the profiling thread
    data_channel: Sender<ProfMsg>,
}

impl ProfilingData {
    /// Create a new profiling instance
    pub const fn new(
        cpu_enabled: Arc<AtomicBool>,
        timely_enabled: Arc<AtomicBool>,
        data_channel: Sender<ProfMsg>,
    ) -> Self {
        Self {
            cpu_enabled,
            timely_enabled,
            data_channel,
        }
    }

    /// Whether CPU profiling is enabled
    pub fn is_cpu_enabled(&self) -> bool {
        self.cpu_enabled.load(Ordering::Relaxed)
    }

    /// Whether timely profiling is enabled
    pub fn is_timely_enabled(&self) -> bool {
        self.timely_enabled.load(Ordering::Relaxed)
    }

    /// Record a profiling message
    pub fn record(&self, event: ProfMsg) {
        let _ = self.data_channel.send(event);
    }
}
