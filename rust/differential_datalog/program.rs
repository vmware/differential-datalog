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
use std::sync::{Arc, Mutex};
use std::error;
use std::result::Result;
use std::collections::hash_map;
// use deterministic hash-map and hash-set, as differential dataflow expects deterministic order of
// creating relations
use std::fmt;
use std::thread;
use std::sync::mpsc;
use fnv::FnvHashMap;
use fnv::FnvHashSet;

use timely;
use timely_communication::initialize::{Configuration, WorkerGuards};
use timely_communication::Allocator;
use timely::dataflow::scopes::*;
use timely::dataflow::operators::probe;
use timely::dataflow::ProbeHandle;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use differential_dataflow::input::{Input,InputSession};
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::*;
use differential_dataflow::Collection;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::lattice::Lattice;
use variable::*;

/* Message buffer for communication with timely threads */
const MSG_BUF_SIZE: usize = 500;

/// Error type returned by this library
#[derive(Debug)]
pub struct Error {
    pub err: String
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        self.err.as_str()
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}

macro_rules! resp_from_error {
    ($($arg:tt)*) => (Result::Err(Error{err: format_args!($($arg)*).to_string()}));
}



/// Result type returned by this library
pub type Response<X> = Result<X, Error>;

/// Value trait describes types that can be stored in a collection
pub trait Val: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + Default + 'static {}
impl<T> Val for T where T: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + Default + 'static {}

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
    pub nodes: Vec<ProgNode<V>>
}

/// Program node is either an individual non-recursive relation or
/// a vector of one or more mutually recursive relations.
#[derive(Clone)]
pub enum ProgNode<V: Val> {
    RelNode{rel: Relation<V>},
    SCCNode{rels: Vec<Relation<V>>}
}

pub type UpdateCallback<V> = Arc<Fn(RelId, &V, bool) + Send + Sync>;

/// Datalog relation.
///
/// defines a set of rules and a set of arrangements with which this relation is used in 
/// rules.  The set of rules can be empty (if this is a ground relation); the set of arrangements 
/// can also be empty if the relation is not used in the RHS of any rules.
#[derive(Clone)]
pub struct Relation<V: Val> {
    /// Relation name; does not have to be unique
    pub name:         String,
    /// `true` is this is an input relation. Input relations are populated by the client 
    /// of the library via `RunningProgram::insert()`, `RunningProgram::delete()` and `RunningProgram::apply_updates()` methods.
    pub input:        bool,
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
    pub change_cb:    UpdateCallback<V>
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
        rel: RelId
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
    thread_handle: thread::JoinHandle<Result<WorkerGuards<()>, String>>,
    transaction_in_progress: bool,
    need_to_flush: bool
}

/* Runtime representation of relation */
struct RelationInstance<V: Val> {
    input: bool,
    /* Set of all elements in the relation. Only maintained for input relations and is used
     * to enforce set semantics (repeated inserts and deletes are enforced). */
    elements: ValSet<V>,
    /* Changes since start of transaction.  Only maintained for input relations and is used to
    * enforce set semantics. */
    delta:    DeltaSet<V>
}

/// A data type to represent and insert and delete commands.  A unified type lets us
/// combine many updates in one message.
#[derive(Debug)]
pub enum Update<V: Val> {
    Insert{relid: RelId, v: V},
    Delete{relid: RelId, v: V}
}

impl<V:Val> Update<V> {
    pub fn relid(&self) -> RelId {
        match self {
            Update::Insert{relid, v: _} => *relid,
            Update::Delete{relid, v: _} => *relid
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

        let h = thread::spawn(move || 
            /* start up timely computation */
            timely::execute(Configuration::Process(nworkers), move |worker: &mut Root<Allocator>| {
                let probe = probe::Handle::new();
                let mut probe1 = probe.clone();
                let worker_index = worker.index();

                let rx = rx.clone();
                let mut sessions = worker.dataflow::<u64,_,_>(|outer: &mut Child<Root<Allocator>, u64>| {
                    let mut sessions : FnvHashMap<RelId, InputSession<u64, V, isize>> = FnvHashMap::default();
                    let mut collections : FnvHashMap<RelId, Collection<Child<Root<Allocator>, u64>,V,isize>> = FnvHashMap::default();
                    let mut arrangements = FnvHashMap::default();
                    for node in &prog.nodes {
                        match node {
                            ProgNode::RelNode{rel:r} => {
                                let (session, mut collection) = outer.new_collection::<V,isize>();
                                if r.input {
                                    sessions.insert(r.id, session);
                                };
                                /* apply rules */
                                for rule in &r.rules {
                                    collection = collection.concat(&prog.mk_rule(rule, |rid| collections.get(&rid), &arrangements));
                                };
                                collection = collection.distinct();
                                /* create arrangements */
                                for (i,arr) in r.arrangements.iter().enumerate() {
                                    arrangements.insert((r.id, i), collection.flat_map(arr.afun).arrange_by_key());
                                };
                                collections.insert(r.id, collection);
                            },
                            ProgNode::SCCNode{rels:rs} => {
                                /* create collections; add them to map; we will overwrite them with
                                 * updated collections returned from the inner scope. */
                                for r in rs.iter() {
                                    let (session, collection) = outer.new_collection::<V,isize>();
                                    if r.input {
                                        sessions.insert(r.id, session);
                                    }
                                    collections.insert(r.id, collection);
                                };
                                /* create a nested scope for mutually recursive relations */
                                let new_collections = outer.scoped(|inner| {
                                    /* create variables for relations defined in the SCC, as well as all
                                     * relations they depend on. */
                                    let mut vars = FnvHashMap::default();
                                    let mut inner_arrangements = FnvHashMap::default();
                                    for dep in Self::dependencies(&rs) {
                                        match dep {
                                            Dep::DepRel(relid) => {
                                                if !vars.contains_key(&relid) {
                                                    vars.insert(relid, Variable::from(&collections.get(&relid).unwrap().enter(inner)));
                                                }
                                            },
                                            Dep::DepArr(arrid) => {
                                                match arrangements.get(&arrid) {
                                                    Some(arr)   => {
                                                        inner_arrangements.insert(arrid, arr.enter(inner));
                                                    },
                                                    None        => {
                                                        if !vars.contains_key(&arrid.0) {
                                                            vars.insert(arrid.0, Variable::from(&collections.get(&arrid.0).unwrap().enter(inner)));
                                                        };
                                                    } // TODO: figure out if there is a safe way to create arrangements inside recursive fragment
                                                };
                                            }
                                        }
                                    };
                                    /* apply rules to variables */
                                    for rel in rs {
                                        for rule in &rel.rules {
                                            let c = prog.mk_rule(rule, |rid|vars.get(&rid).map(|v|&(**v)), &inner_arrangements);
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
                                        arrangements.insert((rel.id, i), collections.get(&rel.id).unwrap().flat_map(arr.afun).arrange_by_key());
                                    };
                                };
                            }
                        };
                    };

                    for (relid, collection) in collections {
                        /* notify client about changes */
                        let cb = prog.get_relation(relid).change_cb.clone();
                        collection.inspect(move |x| {debug_assert!(x.2 == 1 || x.2 == -1); cb(relid, &x.0, x.2 == 1)})
                            .probe_with(&mut probe1);
                    };

                    sessions
                });
                //println!("worker {} started", worker.index());

                /* Only worker 0 receives data */
                if worker_index != 0 {return;};

                let mut epoch: u64 = 0;
                let rx = rx.lock().unwrap();
                loop {
                    match rx.recv() {
                        Err(_)  => {
                            /* Sender hung */
                            break;
                        },
                        Ok(Msg::Update(mut updates)) => {
                            //println!("updates: {:?}", updates);
                            for update in updates.drain(..) {
                                match update {
                                    Update::Insert{relid, v} => {
                                        sessions.get_mut(&relid).unwrap().insert(v);
                                    },
                                    Update::Delete{relid, v} => {
                                        sessions.get_mut(&relid).unwrap().remove(v);
                                    }
                                }
                            };
                            epoch = epoch+1;
                            //print!("epoch: {}\n", epoch);
                            Self::advance(&mut sessions, epoch);
                        },
                        Ok(Msg::Flush) => {
                            //println!("flushing");
                            Self::flush(&mut sessions, &probe, worker);
                            //println!("flushed");
                            flush_ack_send.send(()).unwrap();
                        },
                        Ok(Msg::Stop) => {
                            break;
                        }
                    };
                };
            }));

        //println!("timely computation started");

        let mut rels = FnvHashMap::default();
        for relid in self.input_relations() {
            rels.insert(relid, 
                        RelationInstance{
                            input:    self.get_relation(relid).input,
                            elements: FnvHashSet::default(),
                            delta:    FnvHashMap::default()
                        });
        };

        RunningProgram{
            sender: tx,
            flush_ack: flush_ack_recv,
            relations: rels,
            thread_handle: h,
            transaction_in_progress: false,
            need_to_flush: false
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
        worker: &mut Root<Allocator>) 
    {
        for (_,r) in sessions.into_iter() {
            //print!("flush\n");
            r.flush();
        };
        if let Some((_,session)) = sessions.into_iter().nth(0) {
            while probe.less_than(session.time()) {
                //println!("flush.step");
                worker.step();
            };
        }
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

    /* Lookup arrangement by id */
    fn get_arrangement(&self, arrid: ArrId) -> &Arrangement<V> {
        &self.get_relation(arrid.0).arrangements[arrid.1]
    }
    
    /* Return all relations required to compute rels, including rels themselves */
    fn dependencies(rels: &Vec<Relation<V>>) -> FnvHashSet<Dep> {
        let mut result = FnvHashSet::default();
        for rel in rels {
            result.insert(Dep::DepRel(rel.id));
            for rule in &rel.rules {
                result.insert(Dep::DepRel(rule.rel));
                for xform in &rule.xforms {
                    match xform {
                        XForm::Join{afun: _, arrangement: arrid, jfun: _} => {
                            result.insert(Dep::DepArr(*arrid));
                        },
                        XForm::Antijoin {afun: _, rel: relid} => {
                            result.insert(Dep::DepRel(*relid));
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
    fn mk_rule<'a,S:Scope,T,F>(&'a self,
                               rule: &Rule<V>,
                               lookup_collection: F,
                               arrangements: &'a FnvHashMap<ArrId, Arranged<S,V,V,isize,T>>) -> Collection<S,V> 
        where T : TraceReader<V,V,S::Timestamp,isize> + Clone + 'static,
              S::Timestamp : Lattice,
              F: Fn(RelId) -> Option<&'a Collection<S,V>>
    {
        let first = match lookup_collection(rule.rel) {
            None    => panic!("mk_rule: unknown relation id {}", rule.rel),
            Some(c) => c
        };
        let mut rhs = None;
        for xform in &rule.xforms {
            rhs = match xform {
                XForm::Map{mfun: &f} => {
                    Some(rhs.as_ref().unwrap_or(first).map(f))
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
                    match arrangements.get(&arrid) {
                        Some(arranged) => {
                            Some(rhs.as_ref().unwrap_or(first).
                                 flat_map(af).arrange_by_key().
                                 join_core(arranged, jf))
                        },
                        None      => {
                            let arrangement = self.get_arrangement(*arrid);
                            let arranged = lookup_collection(arrid.0).unwrap().flat_map(arrangement.afun).arrange_by_key();
                            Some(rhs.as_ref().unwrap_or(first).
                                 flat_map(af).arrange_by_key().
                                 join_core(&arranged, jf))
                        }
                    }
                },
                XForm::Antijoin {afun: &af, rel: relid} => {
                    let collection = lookup_collection(*relid).unwrap();
                    Some(rhs.as_ref().unwrap_or(first).
                         flat_map(af).
                         antijoin(&collection).
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
                Err(_) => resp_from_error!("timely thread terminated with an error"),
                Ok(Err(errstr)) => resp_from_error!("timely dataflow error: {}", errstr),
                Ok(Ok(guards)) => {
                    guards.join();
                    Ok(())
                }
            }
        })
    }

    /// Start a transaction.  Does not return a transaction handle, as there
    /// can be at most one transaction in progress at any given time.  Fails 
    /// if there is already a transaction in progress.
    pub fn transaction_start(&mut self) -> Response<()> {
        if self.transaction_in_progress {
            return resp_from_error!("transaction already in progress");
        }; 

        self.transaction_in_progress = true;
        Result::Ok(())
    }

    /// Commit a transaction.
    pub fn transaction_commit(&mut self) -> Response<()> {
        if !self.transaction_in_progress {
            return resp_from_error!("no transaction in progress")
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
            return resp_from_error!("no transaction in progress");
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
    pub fn delete(&mut self, relid: RelId, v: V) -> Response<()> {
        self.apply_updates(vec![Update::Delete {
            relid: relid,
            v:     v
        }])
    }
    
    /// Apply multiple insert and delete operations in one batch.
    /// Updates can only be applied to input relations (see `struct Relation`).
    pub fn apply_updates(&mut self, mut updates: Vec<Update<V>>) -> Response<()> {
        if !self.transaction_in_progress {
            return resp_from_error!("no transaction in progress");
        };

        /* Remove no-op updates to maintain set semantics */
        let mut filtered_updates = Vec::new();
        for upd in updates.drain(..) {
            let mut rel = match self.relations.get_mut(&upd.relid()) {
                None => return resp_from_error!("unknown relation {}", upd.relid()),
                Some(rel) => {
                    if !rel.input {
                        return resp_from_error!("relation {} is not an input relation", upd.relid())
                    };
                    rel
                }
            };
            let pass = match &upd {
                Update::Insert{relid: _, v} => {
                    Self::set_update(&mut rel.elements, &mut rel.delta, &v, true) 
                },
                Update::Delete{relid: _, v} => {
                    Self::set_update(&mut rel.elements, &mut rel.delta, &v, false)
                }
            };
            if pass {
                filtered_updates.push(upd);
            }
        }

        self.send(Msg::Update(filtered_updates))
        .and_then(|_| {
            self.need_to_flush = true;
            Ok(())
        })
    }

    /* Update value set and delta set of an input relation before performin an update.
     * `s` is the current content of the relation.
     * `ds` is delta since start of transaction.
     * `x` is the value being inserted or deleted.
     * `insert` indicates type of update (`true` for insert, `false` for delete).
     * Returns `true` if the update modifies the relation, i.e., it's not a no-op. */
    fn set_update(s: &mut ValSet<V>, ds: &mut DeltaSet<V>, x : &V, insert: bool) -> bool
    {
        //println!("xupd {:?} {}", *x, w);
        if insert {
            let new = s.insert(x.clone());
            if new {
                let e = ds.entry(x.clone());
                match e {
                    hash_map::Entry::Occupied(mut oe) => {
                        debug_assert!(*oe.get_mut() == false);
                        oe.remove_entry();
                    },
                    hash_map::Entry::Vacant(ve) => {ve.insert(true);}
                }
            };
            new
        } else {
            let present = s.remove(x);
            if present {
                let e = ds.entry(x.clone());
                match e {
                    hash_map::Entry::Occupied(mut oe) => {
                        debug_assert!(*oe.get_mut() == true);
                        oe.remove_entry();
                    },
                    hash_map::Entry::Vacant(ve) => {ve.insert(false);}
                }
            };
            present
        }
    }

    /* Returns a reference to relation content.
     * If called in the middle of a transaction, returns state snapshot including changed
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
            Err(_) => resp_from_error!("failed to communicate with timely dataflow thread"),
            Ok(()) => Ok(())
        }
    }

    /* Clear delta sets of all input relations on transaction commit. */
    fn delta_cleanup(&mut self) -> Response<()> {
        for (_, rel) in &mut self.relations {
            if !rel.input {continue};
            rel.delta.clear();
        };
        Ok(())
    }

    /* Reverse all changes recorded in delta sets to rollback the transaction. */
    fn delta_undo(&mut self) -> Response<()> {
        let mut updates = vec![];
        for (relid, rel) in &self.relations {
            if !rel.input {continue};
            for (k, w) in &rel.delta {
                if *w {
                    updates.push(
                        Update::Delete{
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
                    debug_assert!(rel.delta.is_empty());
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
                Err(_) => resp_from_error!("failed to receive flush ack message from timely dataflow thread"),
                Ok(()) => Ok(())
            }
        })
    }
}
