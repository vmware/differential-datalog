use serde::ser::*;
use serde::de::*;
use abomonation::Abomonation;
use std::hash::Hash;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
// use deterministic hash-map and hash-set, as differential dataflow expects deterministic order of
// creating relations
use fnv::FnvHashMap;
use fnv::FnvHashSet;

use timely;
use timely_communication::initialize::Configuration;
use timely_communication::Allocator;
use timely::dataflow::scopes::*;
use timely::dataflow::operators::probe;
use differential_dataflow::input::{Input,InputSession};
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::*;
use differential_dataflow::Collection;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use variable::*;

const NTIMELY_THREADS: usize = 1;

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

pub type ValTraceAgent<S:Scope, V>  = TraceAgent<V, V, S::Timestamp, isize, OrdValSpine<V, V, S::Timestamp, isize>>;

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
//type ValCollection<'a, V> = Collection<Child<'a, Root<Allocator>, u64>, V, isize>;
//type ValArrangement<'a, V, T> = Arranged<Child<'a, Root<Allocator>, u64>, V, V, isize, T>;

/* Runtime representation of relation
 */
pub struct RelationInstance<V: Val> {
    /* Input session to feed data to relation */
    input:    InputSession<u64, V, isize>,
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


impl<V:Val> Program<V> 
{
    pub fn run(&self) {
        let mut elements : FnvHashMap<RelId, ValSet<V>>   = FnvHashMap::default();
        let mut deltas   : FnvHashMap<RelId, DeltaSet<V>> = FnvHashMap::default();

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

        // start up timely computation
        timely::execute(Configuration::Process(NTIMELY_THREADS), move |worker: &mut Root<Allocator>| {
            let probe = probe::Handle::new();
            let mut probe1 = probe.clone();

            let sessions = worker.dataflow::<u64,_,_>(|outer: &mut Child<Root<Allocator>, u64>| {
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
                                                } // TODO: figure out it there is a safe way to create arrangements inside recursive fragment
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
        });
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
