use serde::ser::*;
use serde::de::*;
use abomonation::Abomonation;
use std::hash::Hash;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::error;
use std::result::Result;
// use deterministic hash-map and hash-set, as differential dataflow expects deterministic order of
// creating relations
use std::fmt;
use std::thread;
use std::sync::mpsc;
use fnv::FnvHashMap;
use fnv::FnvHashSet;

use timely;
use timely_communication::initialize::Configuration;
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

const NTIMELY_THREADS: usize = 16;

/* Value trait describes types that can be stored in a collection */
pub trait Val: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + Default + 'static {}
impl<T> Val for T where T: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + Default + 'static {}

//pub trait ValTraceReader<V: Val> : TraceReader<V,V,Product<RootTimestamp,u64>,isize>+Clone+'static {}
//impl<T,V: Val> ValTraceReader<V> for T where T : TraceReader<V,V,Product<RootTimestamp,u64>,isize>+Clone+'static {}

type RelId = usize;
type ArrId = (usize, usize);

/* Datalog program is a vector of strongly connected components (representing mutually recursive
 * rules) or individual non-recursive relations. */
#[derive(Clone)]
pub struct Program<V: Val> {
    pub nodes: Vec<ProgNode<V>>
}

#[derive(Clone)]
pub enum ProgNode<V: Val> {
    RelNode{rel: Relation<V>},
    SCCNode{rels: Vec<Relation<V>>}
}

/* Relation defines a set of rules and a set of arrangements with which this relation is used in 
 * rules.  The set of rules can be empty (if this is a ground relation); the set of arrangements 
 * can also be empty if the relation is not used in the RHS of any rules.  name is the
 * human-readable name of the relation; scc is the index of the strongly connected component of 
 * the dependency graph that the relation belongs to. */
#[derive(Clone)]
pub struct Relation<V: Val> {
    name:         String,
    id:           RelId,      
    rules:        Vec<Rule<V>>,
    arrangements: Vec<Arrangement<V>>
}

/* Function used to map a collection of values to another collection of values */
pub type MapFunc<V>        = fn(V) -> V;

/* Function used to filter a collection of values */
pub type FilterFunc<V>     = fn(&V) -> bool;

/* Function that simultaneously filters and maps a collection */
pub type FilterMapFunc<V>  = fn(V) -> Option<V>;

/* Function that arranges a collection, possibly filtering it  */
pub type ArrangeFunc<V> = fn(V) -> Option<(V,V)>;

/* Function that assembles the result of a join to a new value */
pub type JoinFunc<V> = fn(&V,&V,&V) -> Option<V>;

//pub type ValTraceAgent<S:Scope, V>  = TraceAgent<V, V, S::Timestamp, isize, OrdValSpine<V, V, S::Timestamp, isize>>;

#[derive(Clone)]
pub struct Rule<V: Val> {
    rel:    RelId,        // first relation in the body of the rule
    xforms: Vec<XForm<V>> // chain of transformations
}

#[derive(Clone)]
pub enum XForm<V: Val> {
    Map {
        mfun: &'static MapFunc<V>
    },
    Filter {
        ffun: &'static FilterFunc<V>
    },
    FilterMap {
        fmfun: &'static FilterMapFunc<V>
    },
    Join {
        afun: &'static ArrangeFunc<V>, // arrange the relation before performing join on it
        arrangement: ArrId,            // arrangement to join with
        jfun: &'static JoinFunc<V>     // put together ouput value
    },
    Antijoin {
        afun: &'static ArrangeFunc<V>, // arrange the relation before performing antijoin
        arrangement: ArrId             // arrangement to antijoin with
    }
}

#[derive(Clone)]
pub struct Arrangement<V: Val> {
    name: String,
    afun: &'static ArrangeFunc<V>
}


pub type ValSet<V> = Arc<Mutex<FnvHashSet<V>>>;
pub type DeltaSet<V> = Arc<Mutex<FnvHashMap<V, i8>>>;

/* Running datalog program.
 */
pub struct RunningProgram<V: Val> {
    sender: mpsc::SyncSender<Msg<V>>,
    relations: FnvHashMap<RelId, RelationInstance<V>>,
    transaction_in_progress: bool,
    need_to_flush: bool
}

/* Runtime representation of relation
 */
pub struct RelationInstance<V: Val> {
    /* Input session to feed data to relation */
    //input:    InputSession<u64, V, isize>,
    /* Set of all elements in the relation */
    elements: ValSet<V>,
    /* Changes since start of transaction */
    delta:    DeltaSet<V>
}

#[derive(PartialEq,Eq,Hash)]
enum Dep {
    DepRel(RelId),
    DepArr(ArrId)
}

/* Messages sent to timely worker threads
 */
enum Msg<V: Val> {
    Insert{relid: RelId, v: V},
    Delete{relid: RelId, v: V},
    Flush
}

impl<V:Val> Program<V> 
{
    pub fn run(&self) -> RunningProgram<V> {
        let mut elements : FnvHashMap<RelId, ValSet<V>>   = FnvHashMap::default();
        let mut deltas   : FnvHashMap<RelId, DeltaSet<V>> = FnvHashMap::default();
        let mut elements1 = elements.clone();
        let mut deltas1 = deltas.clone();

        for node in self.nodes.iter() {
            match node {
                ProgNode::RelNode{rel: r} => {
                    elements.insert(r.id, Arc::new(Mutex::new(FnvHashSet::default())));
                    deltas.insert(r.id, Arc::new(Mutex::new(FnvHashMap::default())));
                },
                ProgNode::SCCNode{rels: rs} => {
                    for r in rs.iter() {
                        elements.insert(r.id, Arc::new(Mutex::new(FnvHashSet::default())));
                        deltas.insert(r.id, Arc::new(Mutex::new(FnvHashMap::default())));
                    }
                }
            }
        };
        /* Clone the program, so that it can be moved into the timely computation */
        let prog = self.clone();

        /* Setup channel to communicate with the dataflow.
         * We use sync channel with buffer size 0 to ensure that the receiver finishes 
         * processing commands by the time send() returns.
         */
        let (tx, rx) = mpsc::sync_channel::<Msg<V>>(0);
        let rx = Arc::new(Mutex::new(rx));

        let h = thread::spawn(move || 
        // start up timely computation
            timely::execute(Configuration::Process(NTIMELY_THREADS), move |worker: &mut Root<Allocator>| {
                let probe = probe::Handle::new();
                let mut probe1 = probe.clone();
                let worker_index = worker.index();

                let rx = rx.clone();
                let mut sessions = worker.dataflow::<u64,_,_>(|outer: &mut Child<Root<Allocator>, u64>| {
                    let mut sessions : FnvHashMap<RelId, InputSession<u64, V, isize>> = FnvHashMap::default();
                    let mut collections : FnvHashMap<RelId, Collection<Child<Root<Allocator>, u64>,V,isize>> = FnvHashMap::default();
                    let mut arrangements /*: FnvHashMap<ArrId, Arranged<Child<Root<Allocator>, u64>,V,V,isize,T>>*/ = FnvHashMap::default();
                    for node in &prog.nodes {
                        match node {
                            ProgNode::RelNode{rel:r} => {
                                let (session, mut collection) = outer.new_collection::<V,isize>();
                                sessions.insert(r.id, session);
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
                                    sessions.insert(r.id, session);
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
                                                vars.insert(relid, Variable::from(&collections.get(&relid).unwrap().enter(inner)));
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
                                            let c = prog.mk_rule(rule, |rid|vars.get(&rid).map(|v|&v.current), &inner_arrangements);
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
                        let elems = elements.get(&relid).unwrap().clone();
                        let delts = deltas.get(&relid).unwrap().clone();
                        collection.inspect(move |x| Self::xupd(&elems, &delts, &x.0, x.2)).probe_with(&mut probe1);
                    };

                    sessions
                });
                println!("worker {} started", worker.index());

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
                        Ok(Msg::Insert{relid, v}) => {
                            match sessions.get_mut(&relid) {
                                None => { 
                                    println!("Msg::Insert: unknown relation {}", relid); 
                                    continue;
                                },
                                Some(session) => {
                                    session.insert(v);
                                }
                            };
                            epoch = epoch+1;
                            //print!("epoch: {}\n", epoch);
                            Self::advance(&mut sessions, epoch);
                        },
                        Ok(Msg::Delete{relid, v}) => {
                            match sessions.get_mut(&relid) {
                                None => {
                                    println!("Msg::Insert: unknown relation {}", relid); 
                                    continue;
                                },
                                Some(session) => {
                                    session.remove(v);
                                }
                            };
                            epoch = epoch+1;
                            //print!("epoch: {}\n", epoch);
                            Self::advance(&mut sessions, epoch);
                        },
                        Ok(Msg::Flush) => {
                            Self::flush(&mut sessions, &probe, worker);
                        }
                    };
                };
            }));

        // TODO: check return value of timely::execute()
        println!("timely computation started");

        let mut rels = FnvHashMap::default();
        for (relid, elems) in elements1.drain() {
            rels.insert(relid, 
                        RelationInstance{
                            elements: elems,
                            delta:    deltas1.remove(&relid).unwrap()
                        });
        };

        RunningProgram{
            sender: tx,
            relations: rels,
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
                worker.step();
            };
        }
    }

    fn xupd(s: &ValSet<V>, ds: &DeltaSet<V>, x : &V, w: isize) 
    {
        if w > 0 {
            let new = s.lock().unwrap().insert(x.clone());
            if new {
                let f = |e: &mut i8| if *e == -1 {*e = 0;} else if *e == 0 {*e = 1};
                f(ds.lock().unwrap().entry(x.clone()).or_insert(0));
            };
        } else if w < 0 {
            let present = s.lock().unwrap().remove(x);
            if present {
                let f = |e: &mut i8| if *e == 1 {*e = 0;} else if *e == 0 {*e = -1;};
                f(ds.lock().unwrap().entry(x.clone()).or_insert(0));
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
                        XForm::Antijoin {afun: _, arrangement: arrid} => {
                            result.insert(Dep::DepArr(*arrid));
                        },
                        _ => {}
                    };
                };
            };
        };
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
                XForm::Antijoin {afun: &af, arrangement: arrid} => {
                    match arrangements.get(&arrid) {
                        Some(arranged) => {
                            Some(rhs.as_ref().unwrap_or(first).
                                 flat_map(af).
                                 antijoin(&arranged.as_collection(|k,_|k.clone())).
                                 map(|(_,v)|v))
                        },
                        None      => {
                            let arrangement = self.get_arrangement(*arrid);
                            let arranged = lookup_collection(arrid.0).unwrap().flat_map(arrangement.afun).arrange_by_key();
                            Some(rhs.as_ref().unwrap_or(first).
                                 flat_map(af).
                                 antijoin(&arranged.as_collection(|k,_|k.clone())).
                                 map(|(_,v)|v))
                        }
                    }
                }
            };
        };
        rhs.unwrap_or(first.map(|x|x))
    }
}

#[derive(Debug)]
pub struct Error {
    err: String
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

pub type Response<X> = Result<X, Error>;

macro_rules! resp_from_error {
    ($($arg:tt)*) => (Result::Err(Error{err: format_args!($($arg)*).to_string()}));
}

// TODO: change the interface to take a vector of commands
impl<V:Val> RunningProgram<V> {
    pub fn transaction_start(&mut self) -> Response<()> {
        if self.transaction_in_progress {
            return resp_from_error!("transaction already in progress");
        }; 

        self.transaction_in_progress = true;
        Result::Ok(())
    }

    pub fn transaction_commit(&mut self) -> Response<()> {
        if !self.transaction_in_progress {
            return resp_from_error!("no transaction in progress")
        };

        self.flush();
        self.delta_cleanup();
        self.transaction_in_progress = false;
        Result::Ok(())
    }

    pub fn transaction_rollback(&mut self) -> Response<()> {
        if !self.transaction_in_progress {
            return resp_from_error!("no transaction in progress");
        } 
        
        self.flush();
        self.delta_undo();
        self.transaction_in_progress = false;
        Result::Ok(())
    }

    pub fn insert(&mut self, relid: RelId, v: V) -> Response<()> {
        if !self.transaction_in_progress {
            return resp_from_error!("no transaction in progress");
        } 
        
        //println!("sending");
        self.sender.send(Msg::Insert{
            relid: relid,
            v:     v
        });
        self.need_to_flush = true;
        Ok(())
    }
    
    pub fn delete(&mut self, relid: RelId, v: V) -> Response<()> {
        if !self.transaction_in_progress {
            return resp_from_error!("no transaction in progress");
        };
        
        //println!("deleting");
        self.sender.send(Msg::Delete{
            relid: relid,
            v:     v
        });
        self.need_to_flush = true;
        Ok(())
    }

    /* Returns all elements of a relation */
    pub fn relation_content(&mut self, relid: RelId) -> Response<&ValSet<V>> {
        if !self.transaction_in_progress {
            return resp_from_error!("no transaction in progress");
        };

        self.flush();
        match self.relations.get_mut(&relid) {
            None => resp_from_error!("unknown relation"),
            Some(rel) => Ok(&rel.elements)
        }
    }

    /* Returns delta accumulated by the current transaction */
    pub fn relation_delta(&mut self, relid: RelId) -> Response<&DeltaSet<V>> {
        if !self.transaction_in_progress {
            return resp_from_error!("no transaction in progress");
        };

        self.flush();

        match self.relations.get_mut(&relid) {
            None => resp_from_error!("unknown relation"),
            Some(rel) => Ok(&rel.delta)
        }
    }
 

    fn delta_cleanup(&mut self) {
        for (_, rel) in &self.relations {
            rel.delta.lock().unwrap().clear();
        }
    }

    /* reverse all changes recorded in delta sets */
    fn delta_undo(&mut self) {
        for (relid, rel) in &self.relations {
            let d = rel.delta.lock().unwrap();
            for (k,w) in d.iter() {
                if *w == 1 {
                    self.sender.send(Msg::Delete{
                        relid: *relid,
                        v:     k.clone()
                    });
                } else if *w == -1 {
                    self.sender.send(Msg::Insert{
                        relid: *relid,
                        v:     k.clone()
                    });
                };
            };
        };
        self.flush();

        /* TODO: all deltas must be empty */
    }

    fn flush(&mut self) {
        if !self.need_to_flush {return};
        
        self.sender.send(Msg::Flush);
        self.need_to_flush = false;
    }
}
