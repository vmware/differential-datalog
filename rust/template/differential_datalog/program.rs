//! Datalog program.
//!
//! The client constructs a `struct Program` that describes Datalog relations and rules and
//! calls `Program::run()` to instantiate the program.  The method returns an instance of
//! `RunningProgram` that can be used to interact with the program at runtime.  Interactions
//! include starting, committing or rolling back a transaction and modifying input relations.
//! The engine invokes user-provided callbacks as records are added or removed from relations.
//! `RunningProgram::stop()` terminates the Datalog program destroying all its state.

// TODO: namespace cleanup
// TODO: single input relation

use serde::ser::*;
use serde::de::*;
use abomonation::Abomonation;
use std::hash::Hash;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, RwLock, Barrier};
use std::result::Result;
use std::collections::hash_map;
use std::thread;
use std::time::Duration;
use std::sync::mpsc;
// use deterministic hash-map and hash-set, as differential dataflow expects deterministic order of
// creating relations
use fnv::FnvHashMap;
use fnv::FnvHashSet;

use timely;
use timely::communication::initialize::{Configuration};
use timely::communication::Allocator;
use timely::dataflow::scopes::*;
use timely::dataflow::operators::probe;
use timely::dataflow::ProbeHandle;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::logging::TimelyEvent;
use timely::worker::Worker;
use differential_dataflow::input::{Input,InputSession};
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::*;
use differential_dataflow::Collection;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEvent;
use variable::*;
use profile::*;

/* Message buffer for communication with timely threads */
const MSG_BUF_SIZE: usize = 500;

/* Message buffer for profiling messages */
const PROF_MSD_BUF_SIZE: usize = 1000;

/// Result type returned by this library
pub type Response<X> = Result<X, String>;

/// Value trait describes types that can be stored in a collection
pub trait Val: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + Default + Sync + 'static {}
impl<T> Val for T where T: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + Default + Sync + 'static {}

//pub trait ValTraceReader<V: Val> : TraceReader<V,V,Product<RootTimestamp,u64>,isize>+Clone+'static {}
//impl<T,V: Val> ValTraceReader<V> for T where T : TraceReader<V,V,Product<RootTimestamp,u64>,isize>+Clone+'static {}

/// Unique identifier of a datalog relation
pub type RelId = usize;

/// Unique identifier of an arranged relation.
/// The first element of the tuple identifies relation; the second is the index
/// of arrangement for the given relation.
pub type ArrId = (RelId, usize);


// TODO: add validating constructor for Program:
// - relation id's are unique
// - rules only refer to previously declared relations or relations in the local scc
// - input relations do not occur in LHS of rules
// - all references to arrangements are valid
/// A Datalog program is a vector of nodes representing
/// individual non-recursive relations and stongly connected components
/// comprised of one or more mutually recursive relations.
#[derive(Clone)]
pub struct Program<V: Val> {
    pub nodes: Vec<ProgNode<V>>,
    pub init_data: Vec<(RelId, V)>
}

/// Program node is either an individual non-recursive relation or
/// a vector of one or more mutually recursive relations.
#[derive(Clone)]
pub enum ProgNode<V: Val> {
    RelNode{rel: Relation<V>},
    SCCNode{rels: Vec<Relation<V>>}
}

pub type UpdateCallback<V> = Arc<Fn(RelId, &V, isize) + Send + Sync>;

/// Datalog relation.
///
/// defines a set of rules and a set of arrangements with which this relation is used in
/// rules.  The set of rules can be empty (if this is a ground relation); the set of arrangements
/// can also be empty if the relation is not used in the RHS of any rules.
#[derive(Clone)]
pub struct Relation<V: Val> {
    /// Relation name; does not have to be unique
    pub name:         String,
    /// `true` if this is an input relation. Input relations are populated by the client
    /// of the library via `RunningProgram::insert()`, `RunningProgram::delete()` and `RunningProgram::apply_updates()` methods.
    pub input:        bool,
    /// apply distinct() to this relation
    pub distinct:     bool,
    /// if this `key_func` is present, this indicates that the relation is indexed with a unique
    /// key computed by key_func
    pub key_func:     Option<fn(&V) -> V>,
    /// Unique relation id
    pub id:           RelId,
    /// Rules that define the content of the relation.
    /// Input relations cannot have rules.
    /// Rules can only refer to relations introduced earlier in the program as well as relations in the same strongly connected
    /// component.
    pub rules:        Vec<Rule<V>>,
    /// Arrangements of the relation used to compute other relations.  Index in this vector
    /// along with relation id uniquely identifies the arrangement (see `ArrId`).
    pub arrangements: Vec<Arrangement<V>>,
    /// Callback invoked when an element is added or removed from relation.
    pub change_cb:    Option<UpdateCallback<V>>
}

/// Function type used to map the content of a relation
/// (see `XForm::Map`).
pub type MapFunc<V>        = fn(V) -> V;

/// (see `XForm::FlatMap`).
pub type FlatMapFunc<V>        = fn(V) -> Option<Box<Iterator<Item=V>>>;

/// Function type used to filter a relation
/// (see `XForm::Filter`).
pub type FilterFunc<V>     = fn(&V) -> bool;

/// Function type used to simultaneously filter and map a relation
/// (see `XForm::FilterMap`).
pub type FilterMapFunc<V>  = fn(V) -> Option<V>;

/// Function type used to arrange a relation into key-value pairs
/// (see `XForm::Join`, `XForm::Antijoin`).
pub type ArrangeFunc<V> = fn(V) -> Option<(V,V)>;

/// Function type used to assemble the result of a join into a value.
/// Takes join key and a pair of values from the two joined relation
/// (see `XForm::Join`).
pub type JoinFunc<V> = fn(&V,&V,&V) -> Option<V>;

/// Aggregation function: aggregates multiple values into a single value.
pub type AggFunc<V> = fn(&V, &[(&V, isize)]) -> V;

/// Datalog rule (more precisely, the body of a rule).
#[derive(Clone)]
pub struct Rule<V: Val> {
    /// First relation in the body of the rule
    pub rel:    RelId,
    /// Chain of transformations applied to the relation.  Each subsequent transformation is
    /// applied to the relation produced by previous transformations.
    pub xforms: Vec<XForm<V>>
}

/// Relation transformation.  This is the building block of a Datalog rule (see `struct Rule`).
#[derive(Clone)]
pub enum XForm<V: Val> {
    /// Map a relation
    Map {
        mfun: &'static MapFunc<V>
    },
    /// FlatMap
    FlatMap {
        fmfun: &'static FlatMapFunc<V>
    },
    /// Aggregate
    Aggregate {
        /// group function computes the key to group records by
        grpfun: &'static ArrangeFunc<V>,
        /// aggregation to apply to each group
        aggfun: &'static AggFunc<V>
    },
    /// Filter a relation
    Filter {
        ffun: &'static FilterFunc<V>
    },
    /// Map and filter
    FilterMap {
        fmfun: &'static FilterMapFunc<V>
    },
    /// Join
    Join {
        /// Arrange the relation before performing join on it.
        afun: &'static ArrangeFunc<V>,
        /// Arrangement to join with.
        arrangement: ArrId,
        /// Function used to put together ouput value
        jfun: &'static JoinFunc<V>
    },
    /// Antijoin arranges the input relation using `afun` and retuns a subset of values that
    /// correspond to keys not present in relation `rel`.
    Antijoin {
        /// Arrange the relation before performing antijoin.
        afun: &'static ArrangeFunc<V>,
        /// Relation to antijoin with
        rel: RelId,
        /// Filter-map relation to antijoin with before performing the antijoin
        fmfun: &'static FilterMapFunc<V>
    }
}

/// Describes arrangement of a relation into (key,value) pairs.
#[derive(Clone)]
pub struct Arrangement<V: Val> {
    /// Arrangement name; does not have to be unique
    pub name: String,
    /// Function used to produce arrangement.
    pub afun: &'static ArrangeFunc<V>
}

/* Relation content. */
pub type ValSet<V> = FnvHashSet<V>;

/* Indexed relation content. */
pub type IndexedValSet<V> = FnvHashMap<V,V>;

/* Relation delta */
pub type DeltaSet<V> = FnvHashMap<V, bool>;

/// Runtime representation of a datalog program.
pub struct RunningProgram<V: Val> {
    /* producer side of the channel used to send commands to workers */
    sender: mpsc::SyncSender<Msg<V>>,
    /* channel to signal completion of flush requests */
    flush_ack: mpsc::Receiver<()>,
    relations: FnvHashMap<RelId, RelationInstance<V>>,
    /* timely worker threads */
    thread_handle: thread::JoinHandle<Result<(), String>>,
    transaction_in_progress: bool,
    need_to_flush: bool,
    /* profiling thread */
    prof_thread_handle: thread::JoinHandle<()>,
    /* profiling statistics */
    pub profile: Arc<Mutex<Profile>>
}

/* Runtime representation of relation */
enum RelationInstance<V: Val> {
    Flat {
        /* Set of all elements in the relation. Used to enforce set semantics for input relations
         * (repeated inserts and deletes are ignored). */
        elements: ValSet<V>,
        /* Changes since start of transaction. */
        delta:    DeltaSet<V>
    },
    Indexed {
        key_func: fn(&V) -> V,
        /* Set of all elements in the relation indexed by key. Used to enforce set semantics, 
         * uniqueness of keys, and to query input relations by key. */
        elements: IndexedValSet<V>,
        /* Changes since start of transaction.  Only maintained for input relations and is used to
         * enforce set semantics. */
        delta:    DeltaSet<V>
    }
}

impl<V: Val> RelationInstance<V> {
    pub fn delta(&self) -> &DeltaSet<V>{
        match self {
            RelationInstance::Flat{elements: _, delta} => delta,
            RelationInstance::Indexed{key_func: _, elements: _, delta} => delta
        }
    }
    pub fn delta_mut(&mut self) -> &mut DeltaSet<V>{
        match self {
            RelationInstance::Flat{elements: _, delta} => delta,
            RelationInstance::Indexed{key_func: _, elements: _, delta} => delta
        }
    }
}

/// A data type to represent insert and delete commands.  A unified type lets us
/// combine many updates in one message.
/// 'DeleteValue' takes a complete value to be deleted;
/// 'DeleteKey' takes key only and is only defined for relations with 'key_func'
#[derive(Debug)]
pub enum Update<V: Val> {
    Insert{relid: RelId, v: V},
    DeleteValue{relid: RelId, v: V},
    DeleteKey{relid: RelId, k: V}
}

impl<V:Val> Update<V> {
    pub fn relid(&self) -> RelId {
        match self {
            Update::Insert{relid, v: _}      => *relid,
            Update::DeleteValue{relid, v: _} => *relid,
            Update::DeleteKey{relid, k: _}   => *relid
        }
    }
    pub fn is_delete_key(&self) -> bool {
        match self {
            Update::DeleteKey{relid: _, k: _} => true,
            _ => false
        }
    }
    pub fn key(&self) -> &V {
        match self {
            Update::DeleteKey{relid: _, k}   => k,
            _ => panic!("Update::key: not a DeleteKey command")
        }
    }
}

/* A Datalog relation can depend on other relations and their arrangements.
 */
#[derive(PartialEq,Eq,Hash,Debug)]
enum Dep {
    DepRel(RelId),
    DepArr(ArrId)
}

/* Messages sent to timely worker threads
 */
enum Msg<V: Val> {
    Update(Vec<Update<V>>),
    Flush,
    Stop
}

impl<V:Val> Program<V>
{
    /// Instantiate the program with `nworkers` timely threads.
    pub fn run(&self, nworkers: usize) -> RunningProgram<V> {
        /* Clone the program, so that it can be moved into the timely computation */
        let prog = self.clone();

        /* Setup channel to communicate with the dataflow.
         * We use sync channel with buffer size 0 to ensure that the receiver finishes
         * processing commands by the time send() returns.
         */
        let (tx, rx) = mpsc::sync_channel::<Msg<V>>(MSG_BUF_SIZE);
        let rx = Arc::new(Mutex::new(rx));

        /* Channel to send flush acknowledgements. */
        let (flush_ack_send, flush_ack_recv) = mpsc::sync_channel::<()>(0);
        let flush_ack_send = flush_ack_send.clone();

        /* Profiling channel */
        let (prof_send, prof_recv) = mpsc::sync_channel::<ProfMsg>(PROF_MSD_BUF_SIZE);

        /* Profile data structure */
        let profile = Arc::new(Mutex::new(Profile::new()));
        let profile2 = profile.clone();

        /* Thread to collect profiling data */
        let prof_thread = thread::spawn(move||Self::prof_thread_func(prof_recv, profile2));

        /* Shared timestamp managed by worker 0 and read by all other workers */
        let progress_lock = Arc::new(RwLock::new(Product::new(RootTimestamp,0)));
        let progress_barrier = Arc::new(Barrier::new(nworkers));

        let h = thread::spawn(move ||
            /* start up timely computation */
            timely::execute(Configuration::Process(nworkers), move |worker: &mut Worker<Allocator>| {
                let worker_index = worker.index();
                let probe = probe::Handle::new();
                {
                    let mut probe1 = probe.clone();
                    let prof_send1 = prof_send.clone();
                    let prof_send2 = prof_send.clone();
                    let progress_barrier = progress_barrier.clone();

                    worker.log_register().insert::<TimelyEvent,_>("timely", move |_time, data| {
                        let mut filtered:Vec<((Duration, usize, TimelyEvent), String)> = data.drain(..).filter(|event| match event.2 {
                            TimelyEvent::Operates(_) => true,
                            _ => false
                        }).map(|x|(x, get_prof_context())).collect();
                        if filtered.len() > 0 {
                            //eprintln!("timely event {:?}", filtered);
                            prof_send1.send(ProfMsg::TimelyMessage(filtered.drain(..).collect())).unwrap();
                        }
                    });

                    worker.log_register().insert::<DifferentialEvent,_>("differential/arrange", move |_time, data| {
                        if data.len() == 0 {
                            return;
                        }
                        /* Send update to profiling channel */
                        prof_send2.send(ProfMsg::DifferentialMessage(data.drain(..).collect())).unwrap();
                    });

                    let rx = rx.clone();
                    let mut all_sessions = worker.dataflow::<u64,_,_>(|outer: &mut Child<Worker<Allocator>, u64>| {
                        let mut sessions : FnvHashMap<RelId, InputSession<u64, V, isize>> = FnvHashMap::default();
                        let mut collections : FnvHashMap<RelId, Collection<Child<Worker<Allocator>, u64>,V,isize>> = FnvHashMap::default();
                        let mut arrangements = FnvHashMap::default();
                        for (nodeid, node) in prog.nodes.iter().enumerate() {
                            match node {
                                ProgNode::RelNode{rel:r} => {
                                    let (session, mut collection) = outer.new_collection::<V,isize>();
                                    sessions.insert(r.id, session);
                                    /* apply rules */
                                    for rule in &r.rules {
                                        collection = collection.concat(&prog.mk_rule(rule, |rid| collections.get(&rid), &arrangements, &arrangements));
                                    };
                                    /* don't distinct input collections, as this is already done by the set_update logic */
                                    if !r.input && r.distinct {
                                        collection = with_prof_context(&r.name,
                                                                       ||collection.distinct_total());
                                    }
                                    /* create arrangements */
                                    for (i,arr) in r.arrangements.iter().enumerate() {
                                        with_prof_context(&arr.name,
                                                          ||arrangements.insert((r.id, i), collection.flat_map(arr.afun).arrange_by_key()));
                                    };
                                    collections.insert(r.id, collection);
                                },
                                ProgNode::SCCNode{rels:rs} => {
                                    /* create collections; add them to map; we will overwrite them with
                                     * updated collections returned from the inner scope. */
                                    for r in rs.iter() {
                                        let (session, collection) = outer.new_collection::<V,isize>();
                                        if r.input {
                                            panic!("input relation in nested scope: {}", r.name)
                                        }
                                        sessions.insert(r.id, session);
                                        collections.insert(r.id, collection);
                                    };
                                    /* create a nested scope for mutually recursive relations */
                                    let new_collections = outer.scoped(|inner| {
                                        /* create variables for relations defined in the SCC. */
                                        let mut vars = FnvHashMap::default();
                                        /* arrangements created inside the nested scope */
                                        let mut local_arrangements = FnvHashMap::default();
                                        /* arrangement entered from global scope */
                                        let mut inner_arrangements = FnvHashMap::default();
                                        /* collections entered from global scope */
                                        let mut inner_collections = FnvHashMap::default();
                                        for r in rs.iter() {
                                            vars.insert(&r.id, Variable::from(&collections.get(&r.id).unwrap().enter(inner), &r.name));
                                        };
                                        /* create arrangements */
                                        for rel in rs {
                                            for (i, arr) in rel.arrangements.iter().enumerate() {
                                                /* check if arrangement is actually used inside this node */
                                                if prog.arrangement_used_by_nodes((rel.id, i)).iter().any(|n|*n == nodeid) {
                                                    with_prof_context(
                                                        &format!("local {}", arr.name),
                                                        ||local_arrangements.insert((rel.id, i), vars.get(&rel.id).unwrap().flat_map(arr.afun).arrange_by_key()));
                                                }
                                            }
                                        };
                                        for dep in Self::dependencies(&rs) {
                                            match dep {
                                                Dep::DepRel(relid) => {
                                                    assert!(!vars.contains_key(&relid));
                                                    inner_collections.insert(relid,
                                                                             collections
                                                                             .get(&relid)
                                                                             .unwrap()
                                                                             .enter(inner));
                                                },
                                                Dep::DepArr(arrid) => {
                                                    inner_arrangements.insert(arrid,
                                                                              arrangements.get(&arrid)
                                                                              .expect(&format!("DepArr: unknown arrangement {:?}", arrid))
                                                                              .enter(inner));
                                                }
                                            }
                                        };
                                        /* apply rules to variables */
                                        for rel in rs {
                                            for rule in &rel.rules {
                                                let c = prog.mk_rule(
                                                    rule,
                                                    |rid| vars.get(&rid).map(|v|&(**v)).or(inner_collections.get(&rid)),
                                                    &local_arrangements,
                                                    &inner_arrangements);
                                                vars.get_mut(&rel.id).unwrap().add(&c);
                                            };
                                            /* var.distinct() will be called automatically by var.drop() */
                                        };
                                        /* bring new relations back to the outer scope */
                                        let mut new_collections = FnvHashMap::default();
                                        for rel in rs {
                                            new_collections.insert(rel.id, vars.get(&rel.id).unwrap().leave());
                                        };
                                        new_collections
                                    });
                                    /* add new collections to the map */
                                    collections.extend(new_collections);
                                    /* create arrangements */
                                    for rel in rs {
                                        for (i, arr) in rel.arrangements.iter().enumerate() {
                                            /* only if the arrangement is used outside of this node */
                                            if prog.arrangement_used_by_nodes((rel.id, i)).iter().any(|n|*n != nodeid) {
                                                with_prof_context(
                                                    &format!("global {}", arr.name),
                                                    ||arrangements.insert((rel.id, i), collections.get(&rel.id).unwrap().flat_map(arr.afun).arrange_by_key()));
                                            }
                                        };
                                    };
                                }
                            };
                        };

                        for (relid, collection) in collections {
                            /* notify client about changes */
                            match &prog.get_relation(relid).change_cb {
                                None => {
                                    collection.probe_with(&mut probe1);
                                },
                                Some(cb) => {
                                    let cb = cb.clone();
                                    collection.inspect(move |x| {cb(relid, &x.0, x.2)})
                                                      .probe_with(&mut probe1);
                                }
                            }
                        };

                        sessions
                    });
                    //println!("worker {} started", worker.index());

                    let mut epoch: u64 = 0;

                    // feed initial data to sessions
                    if worker_index == 0 {
                        for (relid, v) in prog.init_data.iter() {
                            all_sessions.get_mut(relid).unwrap().insert(v.clone());
                        }
                        epoch = epoch + 1;
                        Self::advance(&mut all_sessions, epoch);
                        Self::flush(&mut all_sessions, &probe, worker, &progress_lock, &progress_barrier);
                    }

                    // close session handles for non-input sessions
                    let mut sessions: FnvHashMap<RelId, InputSession<u64, V, isize>> =
                        all_sessions.drain().filter(|(relid,_)|prog.get_relation(*relid).input).collect();

                    /* Only worker 0 receives data */
                    if worker_index == 0 {
                        let rx = rx.lock().unwrap();
                        loop {
                            match rx.recv() {
                                Err(_)  => {
                                    /* Sender hung */
                                    eprintln!("Sender hung");
                                    Self::stop_workers(&progress_lock, &progress_barrier);
                                    break;
                                },
                                Ok(Msg::Update(mut updates)) => {
                                    //println!("updates: {:?}", updates);
                                    for update in updates.drain(..) {
                                        match update {
                                            Update::Insert{relid, v} => {
                                                sessions.get_mut(&relid).unwrap().insert(v);
                                            },
                                            Update::DeleteValue{relid, v} => {
                                                sessions.get_mut(&relid).unwrap().remove(v);
                                            },
                                            Update::DeleteKey{relid: _,k: _} => {
                                                // workers don't know about keys
                                                panic!("DeleteKey command received by worker thread")
                                            }
                                        }
                                    };
                                    epoch = epoch+1;
                                    //print!("epoch: {}\n", epoch);
                                    Self::advance(&mut sessions, epoch);
                                },
                                Ok(Msg::Flush) => {
                                    //println!("flushing");
                                    Self::flush(&mut sessions, &probe, worker, &progress_lock, &progress_barrier);
                                    //println!("flushed");
                                    flush_ack_send.send(()).unwrap();
                                },
                                Ok(Msg::Stop) => {
                                    Self::stop_workers(&progress_lock, &progress_barrier);
                                    break;
                                }
                            };
                        }
                    };
                }
                if worker_index != 0 {
                    loop {
                        progress_barrier.wait();
                        let time = *progress_lock.read().unwrap();
                        if time == Product::new(RootTimestamp, 0xffffffffffffffff as u64) {
                            return
                        };
                        while probe.less_than(&time) {
                            if !worker.step() {
                                return
                            };
                        };
                        progress_barrier.wait();
                    }
                }
            }
        ).map(|g| {g.join(); ()})
        );

        //println!("timely computation started");

        let mut rels = FnvHashMap::default();
        for relid in self.input_relations() {
            let rel = self.get_relation(relid);
            if rel.input {
                match rel.key_func {
                    None => {
                        rels.insert(relid,
                                    RelationInstance::Flat{
                                        elements: FnvHashSet::default(),
                                        delta:    FnvHashMap::default()
                                    });
                    },
                    Some(f) => {
                        rels.insert(relid,
                                    RelationInstance::Indexed{
                                        key_func: f,
                                        elements: FnvHashMap::default(),
                                        delta:    FnvHashMap::default()
                                    });
                    }
                };
            }
        };

        RunningProgram{
            sender: tx,
            flush_ack: flush_ack_recv,
            relations: rels,
            thread_handle: h,
            transaction_in_progress: false,
            need_to_flush: false,
            prof_thread_handle: prof_thread,
            profile: profile
        }
    }

    /* Profiler thread function */
    fn prof_thread_func(chan: mpsc::Receiver<ProfMsg>, profile: Arc<Mutex<Profile>>) -> () {
        loop {
            match chan.recv() {
                Ok(msg) => profile.lock().unwrap().update(&msg),
                _ => {
                    //eprintln!("profiling thread exiting");
                    return ()
                }
            }
        }
    }

    /* Advance the epoch on all input sessions */
    fn advance(sessions: &mut FnvHashMap<RelId, InputSession<u64, V, isize>>, epoch : u64) {
        for (_,s) in sessions.into_iter() {
            //print!("advance\n");
            s.advance_to(epoch);
        };
    }

    /* Propagate all changes through the pipeline */
    fn flush(
        sessions: &mut FnvHashMap<RelId, InputSession<u64, V, isize>>,
        probe: &ProbeHandle<Product<RootTimestamp, u64>>,
        worker: &mut Worker<Allocator>,
        progress_lock: &RwLock<Product<RootTimestamp, u64>>,
        progress_barrier: &Barrier)
    {
        for (_,r) in sessions.into_iter() {
            //print!("flush\n");
            r.flush();
        };
        if let Some((_,session)) = sessions.into_iter().nth(0) {
            *progress_lock.write().unwrap() = *session.time();
            progress_barrier.wait();
            while probe.less_than(session.time()) {
                //println!("flush.step");
                worker.step();
            };
            progress_barrier.wait();
        }
    }

    fn stop_workers(progress_lock: &RwLock<Product<RootTimestamp, u64>>,
                    progress_barrier: &Barrier)
    {
        *progress_lock.write().unwrap() = Product::new(RootTimestamp, 0xffffffffffffffff as u64);
        progress_barrier.wait();
    }


    /* Lookup relation by id */
    fn get_relation(&self, relid: RelId) -> &Relation<V> {
        for node in &self.nodes {
            match node {
                ProgNode::RelNode{rel:r} => {
                    if r.id == relid {return r;};
                },
                ProgNode::SCCNode{rels:rs} => {
                    for r in rs {
                        if r.id == relid {return r;};
                    };
                }
            }
        };
        panic!("get_relation({}): relation not found", relid)
    }

    /* indices of program nodes that use arrangement */
    fn arrangement_used_by_nodes(&self, arrid: ArrId) -> Vec<usize> {
        self.nodes.iter().enumerate()
            .filter_map(|(i,n)|
                    if Self::node_uses_arrangement(n, arrid) {
                        Some(i)
                    } else {
                        None
                    }).collect()
    }

    fn node_uses_arrangement(n: &ProgNode<V>, arrid: ArrId) -> bool {
        match n {
            ProgNode::RelNode{rel} => {
                Self::rel_uses_arrangement(rel, arrid)
            },
            ProgNode::SCCNode{rels} => {
                rels.iter().any(|rel|Self::rel_uses_arrangement(rel, arrid))
            }
        }
    }

    fn rel_uses_arrangement(r: &Relation<V>, arrid: ArrId) -> bool {
        r.rules.iter().any(|rule| Self::rule_uses_arrangement(rule, arrid))
    }

    // TODO: update this function if we change how rules use relations
    fn rule_uses_arrangement(r: &Rule<V>, arrid: ArrId) -> bool {
        r.xforms.iter().any(|xform|
                            match xform {
                                XForm::Join{afun: _, arrangement, jfun: _} => { *arrangement == arrid },
                                _ => false
                            })
    }

    /* Returns all input relations of the program */
    fn input_relations(&self) -> Vec<RelId> {
        self.nodes.iter().flat_map(|node| {
            match node {
                ProgNode::RelNode{rel:r} => {
                    if r.input {vec![r.id]} else {vec![]}
                },
                ProgNode::SCCNode{rels:rs} => {
                    for r in rs {
                        if r.input { panic!("input relation in SCCNode"); };
                    };
                    vec![]
                }
            }
        }).collect()
    }

    /* Return all relations required to compute rels, excluding recursive dependencies on rels */
    fn dependencies(rels: &Vec<Relation<V>>) -> FnvHashSet<Dep> {
        let mut result = FnvHashSet::default();
        for rel in rels {
            for rule in &rel.rules {
                if rels.iter().all(|r|r.id != rule.rel) {
                    result.insert(Dep::DepRel(rule.rel));
                };
                for xform in &rule.xforms {
                    match xform {
                        XForm::Join{afun: _, arrangement: arrid, jfun: _} => {
                            if rels.iter().all(|r|r.id != arrid.0) {
                                result.insert(Dep::DepArr(*arrid));
                            }
                        },
                        XForm::Antijoin {afun: _, rel: relid, fmfun: _} => {
                            if rels.iter().all(|r|r.id != *relid) {
                                result.insert(Dep::DepRel(*relid));
                            }
                        },
                        _ => {}
                    };
                };
            };
        };
        //println!("dependencies: {:?}", result);
        result
    }

    /* Compile right-hand-side of a rule to a collection */
    fn mk_rule<'a,S:Scope,T1,T2,F>(&'a self,
                                  rule: &Rule<V>,
                                  lookup_collection: F,
                                  arrangements1: &'a FnvHashMap<ArrId, Arranged<S,V,V,isize,T1>>,
                                  arrangements2: &'a FnvHashMap<ArrId, Arranged<S,V,V,isize,T2>>) -> Collection<S,V>
        where T1 : TraceReader<V,V,S::Timestamp,isize> + Clone + 'static,
              T2 : TraceReader<V,V,S::Timestamp,isize> + Clone + 'static,
              S::Timestamp : Lattice,
              F: Fn(RelId) -> Option<&'a Collection<S,V>>
    {
        let first = lookup_collection(rule.rel).expect(&format!("mk_rule: unknown relation id {:?}", rule.rel));
        let mut rhs = None;
        for xform in &rule.xforms {
            rhs = match xform {
                XForm::Map{mfun: &f} => {
                    Some(rhs.as_ref().unwrap_or(first).map(f))
                },
                XForm::Aggregate{grpfun: &g, aggfun: &a} => {
                    Some(rhs.as_ref().unwrap_or(first).
                         flat_map(g).
                         group(move |key, src, dst| dst.push((a(key, src),1))).
                         map(|(_,v)|v))
                },
                XForm::FlatMap{fmfun: &f} => {
                    Some(rhs.as_ref().unwrap_or(first).
                         flat_map(move |x|
                                  /* TODO: replace this with f(x).into_iter().flatten() when the
                                   * iterator_flatten feature makes it out of experimental API. */
                                  match f(x) {Some(iter) => iter, None => Box::new(None.into_iter())}))
                },
                XForm::Filter{ffun: &f} => {
                    Some(rhs.as_ref().unwrap_or(first).filter(f))
                },
                XForm::FilterMap{fmfun: &f} => {
                    Some(rhs.as_ref().unwrap_or(first).flat_map(f))
                },
                XForm::Join{afun: &af, arrangement: arrid, jfun: &jf} => {
                    match arrangements1.get(&arrid) {
                        Some(arranged) => {
                            with_prof_context(
                                "rule (local)"
                                /*rule.name + jname*/,
                                ||Some(rhs.as_ref().unwrap_or(first).
                                  flat_map(af).arrange_by_key().
                                  join_core(arranged, jf)))
                        },
                        None      => {
                            match arrangements2.get(&arrid) {
                                Some(arranged) => {
                                    with_prof_context(
                                        "rule (global)"
                                        /*rule.name + jname*/,
                                        ||Some(rhs.as_ref().unwrap_or(first).
                                               flat_map(af).arrange_by_key().
                                               join_core(arranged, jf)))
                                },
                                None => {
                                    panic!("XForm::Join: unknown arrangement {:?}", arrid)
                                }
                            }
                        }
                    }
                },
                XForm::Antijoin {afun: &af, rel: relid, fmfun: &fmf} => {
                    let collection = lookup_collection(*relid).unwrap();
                    Some(rhs.as_ref().unwrap_or(first).
                         flat_map(af).
                         antijoin(&collection.flat_map(fmf)).
                         map(|(_,v)|v))
                }
            };
        };
        rhs.unwrap_or(first.map(|x|x))
    }
}

/* Interface to a running datalog computation
 */
/* This should not panic, so that the client has a chance to recover from failures */
// TODO: error messages
impl<V:Val> RunningProgram<V> {
    /// Terminate program, kill worker threads.
    pub fn stop(mut self) -> Response<()> {
        self.flush()
        .and_then(|_| self.send(Msg::Stop))
        .and_then(|_| {
            match self.thread_handle.join() {
                Err(_) => Err(format!("timely thread terminated with an error")),
                Ok(Err(errstr)) => Err(format!("timely dataflow error: {}", errstr)),
                Ok(Ok(())) => {
                    match self.prof_thread_handle.join() {
                        Err(_) => Err(format!("profiling thread terminated with an error")),
                        Ok(_)  => Ok(())
                    }
                }
            }
        })
    }

    /// Start a transaction.  Does not return a transaction handle, as there
    /// can be at most one transaction in progress at any given time.  Fails
    /// if there is already a transaction in progress.
    pub fn transaction_start(&mut self) -> Response<()> {
        if self.transaction_in_progress {
            return Err(format!("transaction already in progress"));
        };

        self.transaction_in_progress = true;
        Result::Ok(())
    }

    /// Commit a transaction.
    pub fn transaction_commit(&mut self) -> Response<()> {
        if !self.transaction_in_progress {
            return Err(format!("no transaction in progress"))
        };

        self.flush()
        .and_then(|_| self.delta_cleanup())
        .and_then(|_| {
            self.transaction_in_progress = false;
            Result::Ok(())
        })
    }

    /// Rollback the transaction, undoing all changes.
    pub fn transaction_rollback(&mut self) -> Response<()> {
        if !self.transaction_in_progress {
            return Err(format!("no transaction in progress"));
        }

        self.flush()
        .and_then(|_| self.delta_undo())
        .and_then(|_| {
            self.transaction_in_progress = false;
            Result::Ok(())
        })
    }

    /// Insert one record into input relation. Relations have set semantics, i.e.,
    /// adding an existing record is a no-op.
    pub fn insert(&mut self, relid: RelId, v: V) -> Response<()> {
        self.apply_updates(vec![Update::Insert {
            relid: relid,
            v:     v
        }])
    }

    /// Remove a record if it exists in the relation.
    pub fn delete_value(&mut self, relid: RelId, v: V) -> Response<()> {
        self.apply_updates(vec![Update::DeleteValue {
            relid: relid,
            v:     v
        }])
    }

    /// Remove a key if it exists in the relation.
    pub fn delete_key(&mut self, relid: RelId, k: V) -> Response<()> {
        self.apply_updates(vec![Update::DeleteKey {
            relid: relid,
            k:     k
        }])
    }

    /// Apply multiple insert and delete operations in one batch.
    /// Updates can only be applied to input relations (see `struct Relation`).
    pub fn apply_updates(&mut self, mut updates: Vec<Update<V>>) -> Response<()> {
        if !self.transaction_in_progress {
            return Err(format!("no transaction in progress"));
        };

        /* Remove no-op updates to maintain set semantics */
        let mut filtered_updates = Vec::new();
        for upd in updates.drain(..) {
            let mut rel = self.relations.get_mut(&upd.relid()).ok_or_else(||format!("unknown input relation {}", upd.relid()))?;
            match rel {
                RelationInstance::Flat{elements, delta} => {
                    Self::set_update(elements, delta, upd, &mut filtered_updates)?
                },
                RelationInstance::Indexed{key_func, elements, delta} => {
                    Self::indexed_set_update(*key_func, elements, delta, upd, &mut filtered_updates)?
                }
            };
        }

        self.send(Msg::Update(filtered_updates))
        .and_then(|_| {
            self.need_to_flush = true;
            Ok(())
        })
    }

    /// Deletes all values in an input table
    pub fn clear_relation(&mut self, relid: RelId) -> Response<()> {
        if !self.transaction_in_progress {
            return Err(format!("no transaction in progress"));
        };

        let upds = {
            let mut rel = self.relations.get_mut(&relid).ok_or_else(||format!("unknown input relation {}", relid))?;
            match rel {
                RelationInstance::Flat{elements, delta: _} => {
                    let mut upds: Vec<Update<V>> = Vec::with_capacity(elements.len());
                    for v in elements.iter() {
                        upds.push(Update::DeleteValue{relid, v: v.clone()});
                    };
                    upds
                },
                RelationInstance::Indexed{key_func: _, elements, delta: _} => {
                    let mut upds: Vec<Update<V>> = Vec::with_capacity(elements.len());
                    for k in elements.keys() {
                        upds.push(Update::DeleteKey{relid, k: k.clone()});
                    };
                    upds
                }
            }
        };
        self.apply_updates(upds)
    }

    /* increment the counter associated with value `x` in the delta-set
     * delta(x) == false => remove entry (equivalen to delta(x):=0)
     * x not in delta => delta(x) := true
     * delta(x) == true => error
     */
    fn delta_inc(ds: &mut DeltaSet<V>, x: &V) {
        let e = ds.entry(x.clone());
        match e {
            hash_map::Entry::Occupied(mut oe) => {
                debug_assert!(*oe.get_mut() == false);
                oe.remove_entry();
            },
            hash_map::Entry::Vacant(ve) => {ve.insert(true);}
        }
    }

    /* reverse of delta_inc */
    fn delta_dec(ds: &mut DeltaSet<V>, key: &V) {
        let e = ds.entry(key.clone());
        match e {
            hash_map::Entry::Occupied(mut oe) => {
                debug_assert!(*oe.get_mut() == true);
                oe.remove_entry();
            },
            hash_map::Entry::Vacant(ve) => {ve.insert(false);}
        }
    }

    /* Update value set and delta set of an input relation before performin an update.
     * `s` is the current content of the relation.
     * `ds` is delta since start of transaction.
     * `x` is the value being inserted or deleted.
     * `insert` indicates type of update (`true` for insert, `false` for delete).
     * Returns `true` if the update modifies the relation, i.e., it's not a no-op. */
    fn set_update(s: &mut ValSet<V>, ds: &mut DeltaSet<V>, upd: Update<V>, updates: &mut Vec<Update<V>>) -> Response<()>
    {
        let ok = match &upd {
            Update::Insert{relid: _, v}      => {
                let new = s.insert(v.clone());
                if new { Self::delta_inc(ds, v); };
                new
            },
            Update::DeleteValue{relid: _, v} => {
                let present = s.remove(&v);
                if present { Self::delta_dec(ds, v); };
                present
            },
            Update::DeleteKey{relid, k: _}   => {
                return Err(format!("Cannot delete by key from relation {} that does not have a primary key", relid));
            }
        };
        if ok {
            updates.push(upd)
        };
        Ok(())
    }

    /* insert:
     *      key exists in `s`:
     *          - error
     *      key not in `s`:
     *          - s.insert(x)
     *          - ds(x)++;
     * delete:
     *      key not in `s`
     *          - return error
     *      key in `s` with value `v`:
     *          - s.delete(key)
     *          - ds(v)--
     */
    fn indexed_set_update(key_func: fn(&V)->V , s: &mut IndexedValSet<V>, ds: &mut DeltaSet<V>, upd: Update<V>, updates: &mut Vec<Update<V>>) -> Response<()>
    {
        match upd {
            Update::Insert{relid, v}      => {
                match s.entry(key_func(&v)) {
                    hash_map::Entry::Occupied(_) => {
                        Err(format!("Insert: duplicate key {:?} in value {:?}", key_func(&v), v))
                    },
                    hash_map::Entry::Vacant(ve) => {
                        ve.insert(v.clone());
                        Self::delta_inc(ds, &v);
                        updates.push(Update::Insert{relid, v});
                        Ok(())
                    }
                }
            },
            Update::DeleteValue{relid, v} => {
                match s.entry(key_func(&v).clone()) {
                    hash_map::Entry::Occupied(oe) => {
                        if *oe.get() != v {
                            Err(format!("DeleteValue: key exists with a different value. Value specified: {:?}; existing value: {:?}", v, oe.get()))
                        } else {
                            Self::delta_dec(ds, oe.get());
                            oe.remove_entry();
                            updates.push(Update::DeleteValue{relid, v});
                            Ok(())
                        }
                    },
                    hash_map::Entry::Vacant(_) => {
                        Err(format!("DeleteValue: key not found {:?}", key_func(&v)))
                    }
                }
            },
            Update::DeleteKey{relid, k}   => {
                match s.entry(k.clone()) {
                    hash_map::Entry::Occupied(oe) => {
                        let old = oe.get().clone();
                        Self::delta_dec(ds, oe.get());
                        oe.remove_entry();
                        updates.push(Update::DeleteValue{relid, v: old});
                        Ok(())
                    },
                    hash_map::Entry::Vacant(_) => {
                        return Err(format!("DeleteKey: key not found {:?}", k))
                    }
                }
            }
        }
    }

    /* Returns a reference to relation content.
     * If called in the middle of a transaction, returns state snapshot including changes
     * made by the current transaction.
     */
    /*pub fn relation_content(&mut self, relid: RelId) -> Response<&ValSet<V>> {
        self.flush().and_then(move |_| {
            match self.relations.get_mut(&relid) {
                None => resp_from_error!("unknown relation {}", relid),
                Some(rel) => Ok(&rel.elements)
            }
        })
    }*/

    /* Returns a reference to delta accumulated by the current transaction
     */
    /*pub fn relation_delta(&mut self, relid: RelId) -> Response<&DeltaSet<V>> {
        if !self.transaction_in_progress {
            return resp_from_error!("no transaction in progress");
        };

        self.flush().and_then(move |_| {
            match self.relations.get_mut(&relid) {
                None => resp_from_error!("unknown relation"),
                Some(rel) => Ok(&rel.delta)
            }
        })
    }*/

    /* Send message to worker thread */
    fn send(&self, msg: Msg<V>) -> Response<()> {
        match self.sender.send(msg) {
            Err(_) => Err(format!("failed to communicate with timely dataflow thread")),
            Ok(()) => Ok(())
        }
    }

    /* Clear delta sets of all input relations on transaction commit. */
    fn delta_cleanup(&mut self) -> Response<()> {
        for (_, rel) in &mut self.relations {
            rel.delta_mut().clear();
        };
        Ok(())
    }

    /* Reverse all changes recorded in delta sets to rollback the transaction. */
    fn delta_undo(&mut self) -> Response<()> {
        let mut updates = vec![];
        for (relid, rel) in &self.relations {
            for (k, w) in rel.delta() {
                if *w {
                    updates.push(
                        Update::DeleteValue{
                            relid: *relid,
                            v:     k.clone()
                        })
                } else {
                    updates.push(
                        Update::Insert{
                            relid: *relid,
                            v:     k.clone()
                        })
                }
            };
        };
        //println!("updates: {:?}", updates);
        self.apply_updates(updates)
            .and_then(|_| self.flush())
            .and_then(|_| {
                /* validation: all deltas must be empty */
                for (_, rel) in &self.relations {
                    //println!("delta: {:?}", *d);
                    debug_assert!(rel.delta().is_empty());
                };
                Ok(())
            })
    }

    /* Propagates all changes through the dataflow pipeline. */
    fn flush(&mut self) -> Response<()> {
        if !self.need_to_flush {return Ok(())};

        self.send(Msg::Flush).and_then(|()| {
            self.need_to_flush = false;
            match self.flush_ack.recv() {
                Err(_) => Err(format!("failed to receive flush ack message from timely dataflow thread")),
                Ok(()) => Ok(())
            }
        })
    }
}
