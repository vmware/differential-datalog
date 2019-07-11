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
use std::sync::atomic::{Ordering, AtomicBool};
use std::result::Result;
use std::collections::hash_map;
use std::thread;
use std::time::Duration;
use std::sync::mpsc;
// use deterministic hash-map and hash-set, as differential dataflow expects deterministic order of
// creating relations
use fnv::FnvHashMap;
use fnv::FnvHashSet;
use std::ops::{Mul,Deref,Add};
use num::One;

use timely;
use timely::communication::initialize::{Configuration};
use timely::communication::Allocator;
use timely::dataflow::scopes::*;
use timely::dataflow::operators::*;
use timely::dataflow::ProbeHandle;
use timely::logging::TimelyEvent;
use timely::worker::Worker;
use timely::order::{Product,TotalOrder,PartialOrder};
use timely::progress::{Timestamp, PathSummary};
use timely::progress::timestamp::Refines;
use differential_dataflow::input::{Input,InputSession};
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::*;
use differential_dataflow::Collection;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEvent;
use differential_dataflow::Data;
use differential_dataflow::difference::Monoid;
use differential_dataflow::difference::Diff;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::AsCollection;

use variable::*;
use profile::*;
use record::Mutator;

type TValAgent<S,V> = TraceAgent<V,V,<S as ScopeParent>::Timestamp,Weight,DefaultValTrace<V,V,<S as ScopeParent>::Timestamp,Weight>>;
type TKeyAgent<S,V> = TraceAgent<V,(),<S as ScopeParent>::Timestamp,Weight,DefaultKeyTrace<V,<S as ScopeParent>::Timestamp,Weight>>;

type TValEnter<'a,P,T,V> = TraceEnter<V,V,<P as ScopeParent>::Timestamp,Weight,TValAgent<P,V>,T>;
type TKeyEnter<'a,P,T,V> = TraceEnter<V,(),<P as ScopeParent>::Timestamp,Weight,TKeyAgent<P,V>,T>;

/* 16-bit timestamp.
 * TODO: get rid of this and use `u16` directly when/if differential implements
 * `Lattice`, `Timestamp`, `PathSummary` traits for `u16`.
 */
#[derive(PartialOrd, PartialEq, Eq, Debug, Default, Clone, Hash, Ord)]
pub struct TS16{pub x: u16}

unsafe_abomonate!(TS16);

impl Mul for TS16 {
    type Output = TS16;
    fn mul(self, rhs: TS16) -> Self::Output {
        TS16{x: self.x * rhs.x}
    }
}

impl Add for TS16 {
    type Output = TS16;

    fn add(self, rhs: TS16) -> Self::Output {
        TS16{x: self.x + rhs.x}
    }
}

impl One for TS16 {
    fn one() -> Self {
        TS16{x: 1}
    }
}

impl TS16 {
     pub const fn max_value() -> TS16 {
         TS16{x: 0xffff}
     }
}

impl PartialOrder for TS16 {
    #[inline(always)] fn less_equal(&self, other: &Self) -> bool { self.x.less_equal(&other.x) }
    #[inline(always)] fn less_than(&self, other: &Self) -> bool { self.x.less_than(&other.x) }
}
impl Lattice for TS16 {
    #[inline(always)] fn minimum() -> Self { TS16{x: u16::min_value()} }
    #[inline(always)] fn join(&self, other: &Self) -> Self { TS16{x: ::std::cmp::max(self.x, other.x)} }
    #[inline(always)] fn meet(&self, other: &Self) -> Self { TS16{x: ::std::cmp::min(self.x, other.x)} }
}

impl Timestamp for TS16 { type Summary = TS16;}
impl PathSummary<TS16> for TS16 {
    #[inline]
    fn results_in(&self, src: &TS16) -> Option<TS16> {
        match self.x.checked_add(src.x) {
            None => None,
            Some(y) => Some(TS16{x: y})
        }
    }
    #[inline]
    fn followed_by(&self, other: &TS16) -> Option<TS16> {
        match self.x.checked_add(other.x) {
            None => None,
            Some(y) => Some(TS16{x: y})
        }
    }
}

/* Outer timestamp */
pub type TS = u64;

/* Timestamp for the nested scope
 * Use 16-bit timestamps for inner scopes to save memory
 */
pub type TSNested = TS16;

// Diff associated with records in differential dataflow
pub type Weight = isize;

/* Message buffer for communication with timely threads */
const MSG_BUF_SIZE: usize = 500;

/* Message buffer for profiling messages */
const PROF_MSG_BUF_SIZE: usize = 10000;

/// Result type returned by this library
pub type Response<X> = Result<X, String>;

/// Value trait describes types that can be stored in a collection
pub trait Val: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + Default + Sync + 'static {}
impl<T> Val for T where T: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + Default + Sync + 'static {}

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

/// Represents a dataflow fragment implemented outside of DDlog directly in
/// differential-dataflow.  Takes the set of already constructed collections and modifies this
/// set, adding new collections.  Note that the transformer can only be applied in the top scope
/// (`Child<'a, Worker<Allocator>, TS>`), as we currently don't have a way to ensure that the
/// transformer in monotonic and thus it may not converge if used in a nested scope.
pub type TransformerFunc<V> = fn() -> Box<for<'a> Fn(&mut FnvHashMap<RelId, Collection<Child<'a, Worker<Allocator>, TS>,V,Weight>>)>;

/// Program node is either an individual non-recursive relation, a transformer application or
/// a vector of one or more mutually recursive relations.
#[derive(Clone)]
pub enum ProgNode<V: Val> {
    Rel{rel: Relation<V>},
    Apply{tfun: TransformerFunc<V>},
    SCC{rels: Vec<Relation<V>>}
}

pub trait CBFn<V>: Fn(RelId, &V, bool) + Send {
    fn clone_boxed(&self) -> Box<dyn CBFn<V>>;
}

impl<T, V> CBFn<V> for T
where
    V: Val,
    T: 'static + Send + Clone + Fn(RelId, &V, bool)
{
    fn clone_boxed(&self) -> Box<dyn CBFn<V>> {
        Box::new(self.clone())
    }
}

impl <V: Val> Clone for Box<dyn CBFn<V>> {
    fn clone(&self) -> Self {
        self.as_ref().clone_boxed()
    }
}

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
    pub change_cb:    Option<Arc<Mutex<Box<dyn CBFn<V>>>>>
}

/// Function type used to map the content of a relation
/// (see `XFormCollection::Map`).
pub type MapFunc<V> = fn(V) -> V;

/// (see `XFormCollection::FlatMap`).
pub type FlatMapFunc<V> = fn(V) -> Option<Box<Iterator<Item=V>>>;

/// Function type used to filter a relation
/// (see `XForm*::Filter`).
pub type FilterFunc<V> = fn(&V) -> bool;

/// Function type used to simultaneously filter and map a relation
/// (see `XFormCollection::FilterMap`).
pub type FilterMapFunc<V> = fn(V) -> Option<V>;

/// Function type used to arrange a relation into key-value pairs
/// (see `XFormArrangement::Join`, `XFormArrangement::Antijoin`).
pub type ArrangeFunc<V> = fn(V) -> Option<(V,V)>;

/// Function type used to assemble the result of a join into a value.
/// Takes join key and a pair of values from the two joined relation
/// (see `XFormArrangement::Join`).
pub type JoinFunc<V> = fn(&V,&V,&V) -> Option<V>;

/// Function type used to assemble the result of a semijoin into a value.
/// Takes join key and value (see `XFormArrangement::Semijoin`).
pub type SemijoinFunc<V> = fn(&V,&V, &()) -> Option<V>;

/// Aggregation function: aggregates multiple values into a single value.
pub type AggFunc<V> = fn(&V, &[(&V, Weight)]) -> V;

/// A Datalog relation or rule can depend on other relations and their
/// arrangements.
#[derive(PartialEq,Eq,Hash,Debug,Clone)]
pub enum Dep {
    Rel(RelId),
    Arr(ArrId)
}

impl Dep
{
    pub fn relid(&self) -> RelId
    {
        match self {
            Dep::Rel(relid)     => *relid,
            Dep::Arr((relid,_)) => *relid
        }
    }
}

/// Transformations, such as maps, flatmaps, filters, joins, etc. are the building blocks of
/// DDlog rules.  Different kinds of transformations can be applied only to flat collections,
/// only to arranged collections, or both.  We therefore use separate types to represent
/// collection and arrangement transformations.
///
/// Note that differential sometimes allows the same kind of transformation to be applied to both
/// collections and arrangements; however the former is implemented on top of the latter and incurs
/// the additional cost of arranging the collection.  We only support the arranged version of these
/// transformations, forcing the user to explicitly arrange the collection if necessary (or, as much
/// as possible, keep the data arranged throughout the chain of transformations).
///
/// `XFormArrangement` - arrangement transformation.
#[derive(Clone)]
pub enum XFormArrangement<V: Val>
{
    /// FlatMap arrangement into a collection
    FlatMap {
        description: String,
        fmfun: &'static FlatMapFunc<V>,
        /// Transformation to apply to resulting collection.
        /// `None` terminates the chain of transformations.
        next: Box<Option<XFormCollection<V>>>,
    },
    FilterMap {
        description: String,
        fmfun: &'static FilterMapFunc<V>,
        /// Transformation to apply to resulting collection.
        /// `None` terminates the chain of transformations.
        next: Box<Option<XFormCollection<V>>>,
    },
    /// Aggregate
    Aggregate {
        description: String,
        /// Filter arrangement before grouping
        ffun: Option<&'static FilterFunc<V>>,
        /// Aggregation to apply to each group.
        aggfun: &'static AggFunc<V>,
        /// Apply transformation to the resulting collection.
        next: Box<Option<XFormCollection<V>>>
    },
    /// Join
    Join {
        description: String,
        /// Filter arrangement before joining
        ffun: Option<&'static FilterFunc<V>>,
        /// Arrangement to join with.
        arrangement: ArrId,
        /// Function used to put together ouput value.
        jfun: &'static JoinFunc<V>,
        /// Join returns a collection: apply `next` transformation to it.
        next: Box<Option<XFormCollection<V>>>
    },
    /// Semijoin
    Semijoin {
        description: String,
        /// Filter arrangement before joining
        ffun: Option<&'static FilterFunc<V>>,
        /// Arrangement to semijoin with.
        arrangement: ArrId,
        /// Function used to put together ouput value.
        jfun: &'static SemijoinFunc<V>,
        /// Join returns a collection: apply `next` transformation to it.
        next: Box<Option<XFormCollection<V>>>
    },
    /// Return a subset of values that correspond to keys not present in `arrangement`.
    Antijoin {
        description: String,
        /// Filter arrangement before joining
        ffun: Option<&'static FilterFunc<V>>,
        /// Arrangement to antijoin with
        arrangement: ArrId,
        /// Antijoin returns a collection: apply `next` transformation to it.
        next: Box<Option<XFormCollection<V>>>
    }
}

impl<V: Val> XFormArrangement<V>
{
    pub fn description(&self) -> &str {
        match self {
            XFormArrangement::FlatMap   {description, ..} => &description,
            XFormArrangement::FilterMap {description, ..} => &description,
            XFormArrangement::Aggregate {description, ..} => &description,
            XFormArrangement::Join      {description, ..} => &description,
            XFormArrangement::Semijoin  {description, ..} => &description,
            XFormArrangement::Antijoin  {description, ..} => &description
        }
    }

    fn dependencies(&self) -> FnvHashSet<Dep>
    {
        match self {
            XFormArrangement::FlatMap{next, ..} => {
                match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                }
            },
            XFormArrangement::FilterMap{next, ..} => {
                match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                }
            },
            XFormArrangement::Aggregate{next, ..} => {
                match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                }
            },
            XFormArrangement::Join{arrangement, next, ..} => {
                let mut deps = match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                };
                deps.insert(Dep::Arr(*arrangement));
                deps
            }
            XFormArrangement::Semijoin{arrangement, next, ..} => {
                let mut deps = match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                };
                deps.insert(Dep::Arr(*arrangement));
                deps
            }
            XFormArrangement::Antijoin{arrangement, next, ..} => {
                let mut deps = match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                };
                deps.insert(Dep::Arr(*arrangement));
                deps
            }
        }
    }
}

/// `XFormCollection` - collection transformation.
#[derive(Clone)]
pub enum XFormCollection<V: Val>
{
    /// Arrange the collection, apply `next` transformation to the resulting collection.
    Arrange {
        description: String,
        afun:        &'static ArrangeFunc<V>,
        next:        Box<XFormArrangement<V>>
    },
    /// Apply `mfun` to each element in the collection
    Map {
        description: String,
        mfun:        &'static MapFunc<V>,
        next:        Box<Option<XFormCollection<V>>>
    },
    /// FlatMap
    FlatMap {
        description: String,
        fmfun:       &'static FlatMapFunc<V>,
        next:        Box<Option<XFormCollection<V>>>
    },
    /// Filter collection
    Filter {
        description: String,
        ffun:        &'static FilterFunc<V>,
        next:        Box<Option<XFormCollection<V>>>
    },
    /// Map and filter
    FilterMap {
        description: String,
        fmfun:       &'static FilterMapFunc<V>,
        next:        Box<Option<XFormCollection<V>>>
    }
}

impl<V: Val> XFormCollection<V>
{
    pub fn description(&self) -> &str {
        match self {
            XFormCollection::Arrange   {description,..} => &description,
            XFormCollection::Map       {description,..} => &description,
            XFormCollection::FlatMap   {description,..} => &description,
            XFormCollection::Filter    {description,..} => &description,
            XFormCollection::FilterMap {description,..} => &description
        }
    }

    pub fn dependencies(&self) -> FnvHashSet<Dep>
    {
        match self {
            XFormCollection::Arrange{next, ..} => {
                next.dependencies()
            },
            XFormCollection::Map{next, ..} => {
                match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                }
            },
            XFormCollection::FlatMap{next, ..} => {
                match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                }
            },
            XFormCollection::Filter{next, ..} => {
                match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                }
            },
            XFormCollection::FilterMap{next, ..} => {
                match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies()
                }
            }
        }
    }
}

/// Datalog rule (more precisely, the body of a rule) starts with a collection
/// or arrangement and applies a chain of transformations to it.
#[derive(Clone)]
pub enum Rule<V: Val> {
    CollectionRule {
        description: String,
        rel: RelId,
        xform: Option<XFormCollection<V>>
    },
    ArrangementRule {
        description: String,
        arr: ArrId,
        xform: XFormArrangement<V>
    }
}

impl<V:Val> Rule<V> {
    pub fn description(&self) -> &str {
        match self {
            Rule::CollectionRule{description,..}  => description.as_ref(),
            Rule::ArrangementRule{description,..} => description.as_ref()
        }
    }

    fn dependencies(&self) -> FnvHashSet<Dep>
    {
        match self {
            Rule::CollectionRule{rel,xform,..} => {
                let mut deps = match xform {
                    None => FnvHashSet::default(),
                    Some(ref x) => x.dependencies()
                };
                deps.insert(Dep::Rel(*rel));
                deps
            },
            Rule::ArrangementRule{arr,xform,..} => {
                let mut deps = xform.dependencies();
                deps.insert(Dep::Arr(*arr));
                deps
            }
        }
    }
}

/// Describes arrangement of a relation.
#[derive(Clone)]
pub enum Arrangement<V: Val> {
    /// Arrange into (key,value) pairs
    Map {
        /// Arrangement name; does not have to be unique
        name: String,
        /// Function used to produce arrangement.
        afun: &'static ArrangeFunc<V>
    },
    /// Arrange into a set of values
    Set {
        /// Arrangement name; does not have to be unique
        name: String,
        /// Function used to produce arrangement.
        fmfun: &'static FilterMapFunc<V>,
        /// Apply distinct_total() before arranging filtered collection.
        distinct: bool
    }
}

impl<V: Val> Arrangement<V> {
    fn name(&self) -> String
    {
        match self {
            Arrangement::Map{name,..} => name.clone(),
            Arrangement::Set{name,..} => name.clone()
        }
    }
}

impl<V: Val> Arrangement<V>
{
    fn build_arrangement_root<S>(&self, collection: &Collection<S,V,Weight>)
        -> ArrangedCollection<S,V,TValAgent<S,V>,TKeyAgent<S,V>>
    where
        S: Scope,
        Collection<S,V,Weight>: ThresholdTotal<S,V,Weight>,
        S::Timestamp: Lattice+Ord+TotalOrder
    {
        match self {
            Arrangement::Map{afun, ..} => {
                ArrangedCollection::Map(collection.flat_map(*afun).arrange_by_key())
            },
            Arrangement::Set{fmfun, distinct, ..} => {
                let filtered = collection.flat_map(*fmfun);
                if *distinct {
                    ArrangedCollection::Set(filtered.threshold_total(|_,c| if c.is_zero() { 0 } else { 1 }).arrange_by_self())
                } else {
                    ArrangedCollection::Set(filtered.arrange_by_self())
                }
            }
        }
    }

    fn build_arrangement<S>(&self, collection: &Collection<S,V,Weight>)
        -> ArrangedCollection<S,V,TValAgent<S,V>,TKeyAgent<S,V>>
    where
        S: Scope,
        S::Timestamp: Lattice+Ord
    {
        match self {
            Arrangement::Map{afun, ..} => {
                ArrangedCollection::Map(collection.flat_map(*afun).arrange_by_key())
            },
            Arrangement::Set{fmfun, distinct, ..} => {
                let filtered = collection.flat_map(*fmfun);
                if *distinct {
                    ArrangedCollection::Set(filtered.threshold(|_,c| if c.is_zero() { 0 } else { 1 }).arrange_by_self())
                } else {
                    ArrangedCollection::Set(filtered.arrange_by_self())
                }
            }
        }
    }

}

enum ArrangedCollection<S,V,T1,T2>
where
    S:Scope,
    V:Val,
    S::Timestamp: Lattice+Ord,
    T1: TraceReader<V,V,S::Timestamp,Weight> + Clone,
    T2: TraceReader<V,(),S::Timestamp,Weight> + Clone
{
    Map ( Arranged<S,V,V,Weight,T1> ),
    Set ( Arranged<S,V,(),Weight,T2> ),
}

impl<S,V> ArrangedCollection<S,V,TValAgent<S,V>,TKeyAgent<S,V>>
where
    S: Scope,
    V: Val,
    S::Timestamp: Lattice+Ord
{
    fn enter<'a>(&self, inner: &Child<'a, S,Product<S::Timestamp,TSNested>>)
        -> ArrangedCollection<Child<'a, S,Product<S::Timestamp,TSNested>>,V,
                              TValEnter<S,Product<S::Timestamp,TSNested>,V>,
                              TKeyEnter<S,Product<S::Timestamp,TSNested>,V>>
    {
        match self {
            ArrangedCollection::Map(arr) => ArrangedCollection::Map(arr.enter(inner)),
            ArrangedCollection::Set(arr) => ArrangedCollection::Set(arr.enter(inner))
        }
    }
}

/* Helper type that represents an arranged collection of one of two
 * types (e.g., an arrangement created in a local scope or entered from
 * the parrent scope) */
enum A<'a,'b,V,P,T>
where
    P:ScopeParent,
    P::Timestamp: Lattice+Ord,
    T: Refines<P::Timestamp>+Lattice+Timestamp+Ord,
    V:Val,
    'a: 'b
{
    Arrangement1(&'b ArrangedCollection<Child<'a,P,T>,V,TValAgent<Child<'a,P,T>,V>,TKeyAgent<Child<'a,P,T>,V>>),
    Arrangement2(&'b ArrangedCollection<Child<'a,P,T>,V,TValEnter<'a,P,T,V>,TKeyEnter<'a,P,T,V>>)
}

struct Arrangements<'a,'b,V,P,T>
where
    P:ScopeParent,
    P::Timestamp: Lattice+Ord,
    T: Refines<P::Timestamp>+Lattice+Timestamp+Ord,
    V:Val,
    'a: 'b
{
    arrangements1: &'b FnvHashMap<ArrId, ArrangedCollection<Child<'a,P,T>,V,TValAgent<Child<'a,P,T>,V>,TKeyAgent<Child<'a,P,T>,V>>>,
    arrangements2: &'b FnvHashMap<ArrId, ArrangedCollection<Child<'a,P,T>,V,TValEnter<'a,P,T,V>,TKeyEnter<'a,P,T,V>>>
}

impl<'a,'b,V,P,T> Arrangements<'a,'b,V,P,T>
where
    P:ScopeParent,
    P::Timestamp: Lattice+Ord,
    T: Refines<P::Timestamp>+Lattice+Timestamp+Ord,
    V:Val,
    'a: 'b
{
    fn lookup_arr(&self, arrid: ArrId) -> A<'a,'b,V,P,T>
    {
        self.arrangements1.get(&arrid)
            .map_or_else(
                ||self.arrangements2.get(&arrid).map(|arr| A::Arrangement2(arr))
                      .expect(&format!("mk_rule: unknown arrangement {:?}", arrid)),
                |arr| A::Arrangement1(arr))
    }
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
    /* CPU profiling enabled (can be expensive) */
    profile_cpu: Arc<AtomicBool>,
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
            RelationInstance::Flat{delta, ..} => delta,
            RelationInstance::Indexed{delta, ..} => delta
        }
    }
    pub fn delta_mut(&mut self) -> &mut DeltaSet<V>{
        match self {
            RelationInstance::Flat{delta, ..} => delta,
            RelationInstance::Indexed{delta, ..} => delta
        }
    }
}

/// A data type to represent insert and delete commands.  A unified type lets us
/// combine many updates in one message.
/// `DeleteValue` takes a complete value to be deleted;
/// `DeleteKey` takes key only and is only defined for relations with 'key_func';
/// `Modify` takes a key and a `Mutator` trait object that represents an update
/// to be applied to the given key.
//#[derive(Debug)]
pub enum Update<V: Val> {
    Insert{relid: RelId, v: V},
    DeleteValue{relid: RelId, v: V},
    DeleteKey{relid: RelId, k: V},
    Modify{relid: RelId, k: V, m: Box<dyn Mutator<V> + Send>}
}

impl<V:Val> Update<V> {
    pub fn relid(&self) -> RelId {
        match self {
            Update::Insert{relid, ..}      => *relid,
            Update::DeleteValue{relid, ..} => *relid,
            Update::DeleteKey{relid, ..}   => *relid,
            Update::Modify{relid, ..}      => *relid,
        }
    }
    pub fn is_delete_key(&self) -> bool {
        match self {
            Update::DeleteKey{..} => true,
            _ => false
        }
    }
    pub fn key(&self) -> &V {
        match self {
            Update::DeleteKey{k, ..} => k,
            Update::Modify{k, ..}    => k,
            _ => panic!("Update::key: not a DeleteKey command")
        }
    }
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
        let (prof_send, prof_recv) = mpsc::sync_channel::<ProfMsg>(PROF_MSG_BUF_SIZE);

        /* Profile data structure */
        let profile = Arc::new(Mutex::new(Profile::new()));
        let profile2 = profile.clone();

        /* Thread to collect profiling data */
        let prof_thread = thread::spawn(move||Self::prof_thread_func(prof_recv, profile2));

        let profile_cpu = Arc::new(AtomicBool::new(false));
        let profile_cpu2 = profile_cpu.clone();

        /* Shared timestamp managed by worker 0 and read by all other workers */
        let progress_lock = Arc::new(RwLock::new(0));
        let progress_barrier = Arc::new(Barrier::new(nworkers));

        let h = thread::spawn(move ||
            /* start up timely computation */
            timely::execute(Configuration::Process(nworkers), move |worker: &mut Worker<Allocator>| {
                let worker_index = worker.index();
                let probe = probe::Handle::new();
                {
                    let mut probe1 = probe.clone();
                    let profile_cpu3 = profile_cpu2.clone();
                    let prof_send1 = prof_send.clone();
                    let prof_send2 = prof_send.clone();
                    let progress_barrier = progress_barrier.clone();

                    worker.log_register().insert::<TimelyEvent,_>("timely", move |_time, data| {
                        let profcpu: &AtomicBool = &*profile_cpu3;
                        /* Filter out events we don't care about to avoid the overhead of sending
                         * the event around just to drop it eventually. */
                        let mut filtered:Vec<((Duration, usize, TimelyEvent), String)> = data.drain(..).filter(|event| match event.2 {
                            TimelyEvent::Operates(_) => true,
                            TimelyEvent::Schedule(_) => profcpu.load(Ordering::Acquire),
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
                    let mut all_sessions = worker.dataflow::<TS,_,_>(|outer: &mut Child<Worker<Allocator>, TS>| {
                        let mut sessions : FnvHashMap<RelId, InputSession<TS, V, Weight>> = FnvHashMap::default();
                        let mut collections : FnvHashMap<RelId, Collection<Child<Worker<Allocator>, TS>,V,Weight>> = FnvHashMap::default();
                        let mut arrangements = FnvHashMap::default();
                        for (nodeid, node) in prog.nodes.iter().enumerate() {
                            match node {
                                ProgNode::Rel{rel} => {
                                    /* Relation may already be in the map if it was created by an `Apply` node */
                                    let mut collection = match collections.remove(&rel.id) {
                                        None => {
                                            let (session, collection) = outer.new_collection::<V,Weight>();
                                            sessions.insert(rel.id, session);
                                            collection
                                        },
                                        Some(c) => c
                                    };
                                    /* apply rules */
                                    let mut rule_collections: Vec<_> = rel.rules.iter().map(|rule| {
                                        prog.mk_rule(rule, |rid| collections.get(&rid),
                                                     Arrangements{arrangements1: &arrangements,
                                                     arrangements2: &FnvHashMap::default()})

                                    }).collect();
                                    rule_collections.push(collection);
                                    collection = with_prof_context(&format!("concatenate rules for {}", rel.name),
                                                                   || concatenate_collections(outer, rule_collections.into_iter()));
                                    /* don't distinct input collections, as this is already done by the set_update logic */
                                    if !rel.input && rel.distinct {
                                        collection = with_prof_context(&format!("{}.distinct_total", rel.name),
                                                                       ||collection.threshold_total(|_,c| if c.is_zero() { 0 } else { 1 }));
                                    }
                                    /* create arrangements */
                                    for (i,arr) in rel.arrangements.iter().enumerate() {
                                        with_prof_context(&arr.name(),
                                                          ||arrangements.insert((rel.id, i), arr.build_arrangement_root(&collection)));
                                    };
                                    collections.insert(rel.id, collection);
                                },
                                ProgNode::Apply{tfun} => {
                                    tfun()(&mut collections);
                                },
                                ProgNode::SCC{rels} => {
                                    /* create collections; add them to map; we will overwrite them with
                                     * updated collections returned from the inner scope. */
                                    for r in rels.iter() {
                                        let (session, collection) = outer.new_collection::<V,Weight>();
                                        if r.input {
                                            panic!("input relation in nested scope: {}", r.name)
                                        }
                                        sessions.insert(r.id, session);
                                        collections.insert(r.id, collection);
                                    };
                                    /* create a nested scope for mutually recursive relations */
                                    let new_collections = outer.scoped("recursive component", |inner| {
                                        /* create variables for relations defined in the SCC. */
                                        let mut vars = FnvHashMap::default();
                                        /* arrangements created inside the nested scope */
                                        let mut local_arrangements = FnvHashMap::default();
                                        /* arrangements entered from global scope */
                                        let mut inner_arrangements = FnvHashMap::default();
                                        /* collections entered from global scope */
                                        let mut inner_collections = FnvHashMap::default();
                                        for r in rels.iter() {
                                            vars.insert(r.id, Variable::from(&collections.get(&r.id).unwrap().enter(inner), &r.name));
                                        };
                                        /* create arrangements */
                                        for rel in rels {
                                            for (i, arr) in rel.arrangements.iter().enumerate() {
                                                /* check if arrangement is actually used inside this node */
                                                if prog.arrangement_used_by_nodes((rel.id, i)).iter().any(|n|*n == nodeid) {
                                                    with_prof_context(
                                                        &format!("local {}", arr.name()),
                                                        ||local_arrangements.insert((rel.id, i),
                                                                                    arr.build_arrangement(vars.get(&rel.id).unwrap().deref())));
                                                }
                                            }
                                        };
                                        for dep in Self::dependencies(&rels) {
                                            match dep {
                                                Dep::Rel(relid) => {
                                                    assert!(!vars.contains_key(&relid));
                                                    inner_collections.insert(relid,
                                                                             collections
                                                                             .get(&relid)
                                                                             .unwrap()
                                                                             .enter(inner));
                                                },
                                                Dep::Arr(arrid) => {
                                                    inner_arrangements.insert(arrid,
                                                                              arrangements.get(&arrid)
                                                                              .expect(&format!("Arr: unknown arrangement {:?}", arrid))
                                                                              .enter(inner));
                                                }
                                            }
                                        };
                                        /* apply rules to variables */
                                        for rel in rels {
                                            for rule in &rel.rules {
                                                let c = prog.mk_rule(
                                                    rule,
                                                    |rid| vars.get(&rid).map(|v|&(**v)).or(inner_collections.get(&rid)),
                                                    Arrangements{arrangements1: &local_arrangements,
                                                                 arrangements2: &inner_arrangements});
                                                vars.get_mut(&rel.id).unwrap().add(&c);
                                            };
                                            /* var.distinct() will be called automatically by var.drop() */
                                        };
                                        /* bring new relations back to the outer scope */
                                        let mut new_collections = FnvHashMap::default();
                                        for rel in rels {
                                            new_collections.insert(rel.id, vars.get(&rel.id).unwrap().leave());
                                        };
                                        new_collections
                                    });
                                    /* add new collections to the map */
                                    collections.extend(new_collections);
                                    /* create arrangements */
                                    for rel in rels {
                                        for (i, arr) in rel.arrangements.iter().enumerate() {
                                            /* only if the arrangement is used outside of this node */
                                            if prog.arrangement_used_by_nodes((rel.id, i)).iter().any(|n|*n != nodeid) {
                                                with_prof_context(
                                                    &format!("global {}", arr.name()),
                                                    ||arrangements.insert((rel.id, i), arr.build_arrangement(collections.get(&rel.id).unwrap())));
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
                                    let cb = cb.lock().unwrap().clone();
                                    collection.inspect(move |x| {
                                        debug_assert!(x.2 == 1 || x.2 == -1);
                                        cb(relid, &x.0, x.2 == 1)
                                    }).probe_with(&mut probe1);
                                }
                            }
                        };
                        sessions
                    });
                    //println!("worker {} started", worker.index());

                    let mut epoch: TS = 0;

                    // feed initial data to sessions
                    if worker_index == 0 {
                        for (relid, v) in prog.init_data.iter() {
                            all_sessions.get_mut(relid).unwrap().update(v.clone(), 1);
                        }
                        epoch = epoch + 1;
                        Self::advance(&mut all_sessions, epoch);
                        Self::flush(&mut all_sessions, &probe, worker, &*progress_lock, &progress_barrier);
                        flush_ack_send.send(()).unwrap();
                    }

                    // close session handles for non-input sessions
                    let mut sessions: FnvHashMap<RelId, InputSession<TS, V, Weight>> =
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
                                                sessions.get_mut(&relid).unwrap().update(v, 1);
                                            },
                                            Update::DeleteValue{relid, v} => {
                                                sessions.get_mut(&relid).unwrap().update(v, -1);
                                            },
                                            Update::DeleteKey{..} => {
                                                // workers don't know about keys
                                                panic!("DeleteKey command received by worker thread")
                                            },
                                            Update::Modify{..} => {
                                                panic!("Modify command received by worker thread")
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
                        if time == /*0xffffffffffffffff*/TS::max_value() {
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
        ).map(|g| {g.join();})
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
        /* Wait for the initial transaction to complete */
        flush_ack_recv.recv().unwrap();

        RunningProgram{
            sender: tx,
            flush_ack: flush_ack_recv,
            relations: rels,
            thread_handle: h,
            transaction_in_progress: false,
            need_to_flush: false,
            profile_cpu,
            prof_thread_handle: prof_thread,
            profile,
        }
    }

    /* Profiler thread function */
    fn prof_thread_func(chan: mpsc::Receiver<ProfMsg>, profile: Arc<Mutex<Profile>>) {
        loop {
            match chan.recv() {
                Ok(msg) => profile.lock().unwrap().update(&msg),
                _ => {
                    //eprintln!("profiling thread exiting");
                    return
                }
            }
        }
    }

    /* Advance the epoch on all input sessions */
    fn advance(sessions: &mut FnvHashMap<RelId, InputSession<TS, V, Weight>>, epoch : TS) {
        for (_,s) in sessions.into_iter() {
            //print!("advance\n");
            s.advance_to(epoch);
        };
    }

    /* Propagate all changes through the pipeline */
    fn flush(
        sessions: &mut FnvHashMap<RelId, InputSession<TS, V, Weight>>,
        probe: &ProbeHandle<TS>,
        worker: &mut Worker<Allocator>,
        progress_lock: &RwLock<TS>,
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

    fn stop_workers(progress_lock: &RwLock<TS>,
                    progress_barrier: &Barrier)
    {
        *progress_lock.write().unwrap() = TS::max_value();
        progress_barrier.wait();
    }


    /* Lookup relation by id */
    fn get_relation(&self, relid: RelId) -> &Relation<V> {
        for node in &self.nodes {
            match node {
                ProgNode::Rel{rel:r} => {
                    if r.id == relid {return r;};
                },
                ProgNode::Apply{..} => {},
                ProgNode::SCC{rels:rs} => {
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

    fn node_uses_arrangement(n: &ProgNode<V>, arrid: ArrId) -> bool
    {
        match n {
            ProgNode::Rel{rel} => {
                Self::rel_uses_arrangement(rel, arrid)
            },
            ProgNode::Apply{..} => { false },
            ProgNode::SCC{rels} => {
                rels.iter().any(|rel|Self::rel_uses_arrangement(rel, arrid))
            }
        }
    }

    fn rel_uses_arrangement(r: &Relation<V>, arrid: ArrId) -> bool
    {
        r.rules.iter().any(|rule| Self::rule_uses_arrangement(rule, arrid))
    }

    fn rule_uses_arrangement(r: &Rule<V>, arrid: ArrId) -> bool
    {
        r.dependencies().contains(&Dep::Arr(arrid))
    }

    /* Returns all input relations of the program */
    fn input_relations(&self) -> Vec<RelId>
    {
        self.nodes.iter().flat_map(|node| {
            match node {
                ProgNode::Rel{rel:r} => {
                    if r.input {vec![r.id]} else {vec![]}
                },
                ProgNode::Apply{..} => { vec![] },
                ProgNode::SCC{rels:rs} => {
                    for r in rs {
                        if r.input { panic!("input relation in SCC"); };
                    };
                    vec![]
                }
            }
        }).collect()
    }

    /* Return all relations required to compute rels, excluding recursive dependencies on rels */
    fn dependencies(rels: &Vec<Relation<V>>) -> FnvHashSet<Dep>
    {
        let mut result = FnvHashSet::default();
        for rel in rels {
            for rule in &rel.rules {
                result = result.union(&rule.dependencies()).cloned().collect();
            }
        };
        let filtered = result.drain().filter(|d| rels.iter().all(|r|r.id != d.relid())).collect();
        filtered
    }

    fn xform_collection<'a,'b,P,T>(col         : Collection<Child<'a, P, T>,V,Weight>,
                                   xform       : &Option<XFormCollection<V>>,
                                   arrangements: &Arrangements<'a,'b,V,P,T>) -> Collection<Child<'a, P,T>,V,Weight>
    where
        P  : ScopeParent,
        P::Timestamp : Lattice,
        T: Refines<P::Timestamp>+Lattice+Timestamp+Ord
    {
        match xform {
            None => col,
            Some(ref x) => Self::xform_collection_ref(&col, x, arrangements)
        }
    }

    fn xform_collection_ref<'a,'b,P,T>(col         : &Collection<Child<'a,P,T>,V,Weight>,
                                       xform       : &XFormCollection<V>,
                                       arrangements: &Arrangements<'a,'b,V,P,T>) -> Collection<Child<'a,P,T>,V,Weight>
    where
        P  : ScopeParent,
        P::Timestamp : Lattice,
        T: Refines<P::Timestamp>+Lattice+Timestamp+Ord
    {
        match xform {
            XFormCollection::Arrange{description, afun, ref next} => {
                let arr = with_prof_context(
                    &description,
                    ||col.flat_map(*afun).arrange_by_key());
                Self::xform_arrangement(&arr, &*next, arrangements)
            },
            XFormCollection::Map{description, mfun, ref next} => {
                let mapped = with_prof_context(
                    &description,
                    ||col.map(*mfun));
                Self::xform_collection(mapped, &*next, arrangements)
            },
            XFormCollection::FlatMap{description, fmfun: &fmfun, ref next} => {
                let flattened = with_prof_context(
                    &description,
                    ||col.flat_map(move |x|
                                   /* TODO: replace this with f(x).into_iter().flatten() when the
                                    * iterator_flatten feature makes it out of experimental API. */
                                   match fmfun(x) {Some(iter) => iter, None => Box::new(None.into_iter())}));
                Self::xform_collection(flattened, &*next, arrangements)
            },
            XFormCollection::Filter{description, ffun: &ffun, ref next} => {
                let filtered = with_prof_context(
                    &description,
                    ||col.filter(ffun));
                Self::xform_collection(filtered, &*next, arrangements)
            },
            XFormCollection::FilterMap{description, fmfun: &fmfun, ref next} => {
                let flattened = with_prof_context(
                    &description,
                    ||col.flat_map(fmfun));
                Self::xform_collection(flattened, &*next, arrangements)
            }
        }
    }

    fn xform_arrangement<'a,'b,P,T,TR>(arr         : &Arranged<Child<'a,P,T>,V,V,Weight,TR>,
                                       xform       : &XFormArrangement<V>,
                                       arrangements: &Arrangements<'a,'b,V,P,T>) -> Collection<Child<'a,P,T>,V,Weight>
    where
        P  : ScopeParent,
        P::Timestamp : Lattice,
        T: Refines<P::Timestamp>+Lattice+Timestamp+Ord,
        TR  : TraceReader<V,V,T,Weight> + Clone + 'static,
    {
        match xform {
            XFormArrangement::FlatMap{description, fmfun: &fmfun, next} => {
                with_prof_context(
                    &description,
                    ||Self::xform_collection(
                        arr.flat_map_ref(move |_,v|match fmfun(v.clone()) {Some(iter) => iter, None => Box::new(None.into_iter())}),
                        &*next, arrangements))
            },
            XFormArrangement::FilterMap{description, fmfun: &fmfun, next} => {
                with_prof_context(
                    &description,
                    ||Self::xform_collection(
                        arr.flat_map_ref(move |_,v|fmfun(v.clone())),
                        &*next, arrangements))
            },
            XFormArrangement::Aggregate{description, ffun, aggfun: &aggfun, next} => {
                let col = with_prof_context(
                    &description,
                    ||ffun.map_or_else(||arr.reduce(move |key, src, dst| dst.push((aggfun(key, src),1)))
                                            .map(|(_,v)|v),
                                       |f|arr.filter(move |_,v|f(v))
                                             .reduce(move |key, src, dst| dst.push((aggfun(key, src),1)))
                                             .map(|(_,v)|v)));
                Self::xform_collection(col, &*next, arrangements)
            },
            XFormArrangement::Join{description, ffun, arrangement, jfun: &jfun, next} => {
                match arrangements.lookup_arr(*arrangement) {
                    A::Arrangement1(ArrangedCollection::Map(arranged)) => {
                        let col = with_prof_context(
                            &description,
                            ||ffun.map_or_else(||arr.join_core(arranged, jfun),
                                               |f|arr.filter(move |_,v|f(v)).join_core(arranged, jfun)));
                        Self::xform_collection(col, &*next, arrangements)
                    },
                    A::Arrangement2(ArrangedCollection::Map(arranged)) => {
                        let col = with_prof_context(
                            &description,
                            ||ffun.map_or_else(||arr.join_core(arranged, jfun),
                                               |f|arr.filter(move |_,v|f(v)).join_core(arranged, jfun)));
                        Self::xform_collection(col, &*next, arrangements)
                    },

                    _ => panic!("Join: not a map arrangement {:?}", arrangement)
                }
            },
            XFormArrangement::Semijoin{description, ffun, arrangement, jfun: &jfun, next} => {
                match arrangements.lookup_arr(*arrangement) {
                    A::Arrangement1(ArrangedCollection::Set(arranged)) => {
                        let col = with_prof_context(
                            &description,
                            ||ffun.map_or_else(||arr.join_core(arranged, jfun),
                                               |f|arr.filter(move |_,v|f(v)).join_core(arranged, jfun)));
                        Self::xform_collection(col, &*next, arrangements)
                    },
                    A::Arrangement2(ArrangedCollection::Set(arranged)) => {
                        let col = with_prof_context(
                            &description,
                            ||ffun.map_or_else(||arr.join_core(arranged, jfun),
                                               |f|arr.filter(move |_,v|f(v)).join_core(arranged, jfun)));
                        Self::xform_collection(col, &*next, arrangements)
                    },
                    _ => panic!("Semijoin: not a set arrangement {:?}", arrangement)
                }
            },
            XFormArrangement::Antijoin{description, ffun, arrangement, next} => {
                match arrangements.lookup_arr(*arrangement) {
                    A::Arrangement1(ArrangedCollection::Set(arranged)) => {
                        let col = with_prof_context(
                            &description,
                            ||ffun.map_or_else(||antijoin_arranged(&arr,arranged).map(|(_,v)|v),
                                               |f|antijoin_arranged(&arr.filter(move |_,v|f(v)),arranged).map(|(_,v)|v)));
                        Self::xform_collection(col, &*next, arrangements)
                    },
                    A::Arrangement2(ArrangedCollection::Set(arranged)) => {
                        let col = with_prof_context(
                            &description,
                            ||ffun.map_or_else(||antijoin_arranged(&arr,arranged).map(|(_,v)|v),
                                               |f|antijoin_arranged(&arr.filter(move |_,v|f(v)),arranged).map(|(_,v)|v)));
                        Self::xform_collection(col, &*next, arrangements)
                    },
                    _ => panic!("Antijoin: not a set arrangement {:?}", arrangement)
                }
            }
        }
    }

    /* Compile right-hand-side of a rule to a collection */
    fn mk_rule<'a,'b,P,T,F>(
        &self,
        rule: &Rule<V>,
        lookup_collection: F,
        arrangements: Arrangements<'a,'b,V,P,T>) -> Collection<Child<'a,P,T>,V,Weight>
    where
        P: ScopeParent + 'a,
        P::Timestamp : Lattice,
        T: Refines<P::Timestamp>+Lattice+Timestamp+Ord,
        F: Fn(RelId) -> Option<&'b Collection<Child<'a,P,T>,V,Weight>>,
        'a: 'b
    {
        match rule {
            Rule::CollectionRule{rel, xform: None, ..} => {
                let collection = lookup_collection(*rel).expect(&format!("mk_rule: unknown relation {:?}", rel));
                let rel_name = &self.get_relation(*rel).name;
                with_prof_context(
                    format!("{} clone", rel_name).as_ref(),
                    ||collection.map(|x|x))

            },
            Rule::CollectionRule{rel, xform: Some(x), ..} => {
                Self::xform_collection_ref(lookup_collection(*rel).expect(&format!("mk_rule: unknown relation {:?}", rel)),
                                           x, &arrangements)
            },
            Rule::ArrangementRule{arr, xform, ..} => {
                match arrangements.lookup_arr(*arr) {
                    A::Arrangement1(ArrangedCollection::Map(arranged)) => {
                        Self::xform_arrangement(arranged, xform, &arrangements)
                    },
                    A::Arrangement2(ArrangedCollection::Map(arranged)) => {
                        Self::xform_arrangement(arranged, xform, &arrangements)
                    },
                    _ => panic!("Rule starts with a set arrangement {:?}", *arr)
                }
            }
        }
    }
}

/* Interface to a running datalog computation
 */
/* This should not panic, so that the client has a chance to recover from failures */
// TODO: error messages
impl<V:Val> RunningProgram<V> {
    /// Controls forwarding of `TimelyEvent::Schedule` event to the CPU
    /// profiling thread.
    /// `enable = true`  - enables forwarding. This can be expensive in large dataflows.
    /// `enable = false` - disables forwarding.
    pub fn enable_cpu_profiling(&self, enable: bool) {
        self.profile_cpu.store(enable, Ordering::SeqCst);
    }

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
            return Err(format!("transaction_commit: no transaction in progress"))
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
            return Err(format!("transacion_rollback: no transaction in progress"));
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
        }].into_iter())
    }

    /// Remove a record if it exists in the relation.
    pub fn delete_value(&mut self, relid: RelId, v: V) -> Response<()> {
        self.apply_updates(vec![Update::DeleteValue {
            relid: relid,
            v:     v
        }].into_iter())
    }

    /// Remove a key if it exists in the relation.
    pub fn delete_key(&mut self, relid: RelId, k: V) -> Response<()> {
        self.apply_updates(vec![Update::DeleteKey {
            relid: relid,
            k:     k
        }].into_iter())
    }

    /// Modify a key if it exists in the relation.
    pub fn modify_key(&mut self, relid: RelId, k: V, m: Box<dyn Mutator<V> + Send>) -> Response<()> {
        self.apply_updates(vec![Update::Modify {
            relid: relid,
            k:     k,
            m:     m
        }].into_iter())
    }

    /// Apply multiple insert and delete operations in one batch.
    /// Updates can only be applied to input relations (see `struct Relation`).
    pub fn apply_updates<I: Iterator<Item=Update<V>>>(&mut self, updates: I) -> Response<()> {
        if !self.transaction_in_progress {
            return Err(format!("apply_updates: no transaction in progress"));
        };

        /* Remove no-op updates to maintain set semantics */
        let mut filtered_updates = Vec::new();
        for upd in updates {
            let rel = self.relations.get_mut(&upd.relid()).ok_or_else(||format!("unknown input relation {}", upd.relid()))?;
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
            return Err(format!("clear_relation: no transaction in progress"));
        };

        let upds = {
            let rel = self.relations.get_mut(&relid).ok_or_else(||format!("unknown input relation {}", relid))?;
            match rel {
                RelationInstance::Flat{elements, ..} => {
                    let mut upds: Vec<Update<V>> = Vec::with_capacity(elements.len());
                    for v in elements.iter() {
                        upds.push(Update::DeleteValue{relid, v: v.clone()});
                    };
                    upds
                },
                RelationInstance::Indexed{elements, ..} => {
                    let mut upds: Vec<Update<V>> = Vec::with_capacity(elements.len());
                    for k in elements.keys() {
                        upds.push(Update::DeleteKey{relid, k: k.clone()});
                    };
                    upds
                }
            }
        };
        self.apply_updates(upds.into_iter())
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
            Update::Insert{v, ..}           => {
                let new = s.insert(v.clone());
                if new { Self::delta_inc(ds, v); };
                new
            },
            Update::DeleteValue{v, ..}      => {
                let present = s.remove(&v);
                if present { Self::delta_dec(ds, v); };
                present
            },
            Update::DeleteKey{relid, ..}    => {
                return Err(format!("Cannot delete by key from relation {} that does not have a primary key", relid));
            },
            Update::Modify{relid, ..}       => {
                return Err(format!("Cannot modify record in relation {} that does not have a primary key", relid));
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
            },
            Update::Modify{relid, k, m}   => {
                match s.entry(k.clone()) {
                    hash_map::Entry::Occupied(mut oe) => {
                        let new = oe.get_mut();
                        let old = new.clone();
                        m.mutate(new)?;
                        Self::delta_dec(ds, &old);
                        updates.push(Update::DeleteValue{relid, v: old});
                        Self::delta_inc(ds, &new);
                        updates.push(Update::Insert{relid, v: new.clone()});
                        Ok(())
                    },
                    hash_map::Entry::Vacant(_) => {
                        return Err(format!("Modify: key not found {:?}", k))
                    }
                }
            }
        }
    }

    /* Returns a reference to indexed input relation content.
     * If called in the middle of a transaction, returns state snapshot including changes
     * made by the current transaction.
     */
    pub fn get_input_relation_index(&self, relid: RelId) -> Response<&IndexedValSet<V>> {
        match self.relations.get(&relid) {
            None => {
                Err(format!("unknown relation {}", relid))
            },
            Some(RelationInstance::Flat{..}) => {
                Err(format!("not an indexed relation {}", relid))
            },
            Some(RelationInstance::Indexed{elements, ..}) => {
                Ok(elements)
            },
        }
    }

    /* Returns a reference to a flat input relation content.
     * If called in the middle of a transaction, returns state snapshot including changes
     * made by the current transaction.
     */
    pub fn get_input_relation_data(&self, relid: RelId) -> Response<&ValSet<V>> {
        match self.relations.get(&relid) {
            None => {
                Err(format!("unknown relation {}", relid))
            },
            Some(RelationInstance::Indexed{..}) => {
                Err(format!("not a flat relation {}", relid))
            },
            Some(RelationInstance::Flat{elements, ..}) => {
                Ok(elements)
            },
        }
    }

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
            // first delete, then insert to avoid duplicate key
            // errors in `apply_updates()`
            for (k, w) in rel.delta() {
                if *w {
                    updates.push(
                        Update::DeleteValue{
                            relid: *relid,
                            v:     k.clone()
                        })
                };
            };
            for (k, w) in rel.delta() {
                if !*w {
                    updates.push(
                        Update::Insert{
                            relid: *relid,
                            v:     k.clone()
                        })
                }
            };
        };
        //println!("updates: {:?}", updates);
        self.apply_updates(updates.into_iter())
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

// Versions of semijoin and antijoin operators that take arrangement instead of collection.
fn semijoin_arranged<G,K,V,R1,R2,T1,T2>(arranged: &Arranged<G, K, V, R1, T1>,
                                        other: &Arranged<G, K, (), R2, T2>) -> Collection<G, (K, V), <R1 as Mul<R2>>::Output>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    T1: TraceReader<K,V,G::Timestamp,R1> + Clone + 'static,
    T2: TraceReader<K,(),G::Timestamp,R2> + Clone + 'static,
    K: Data+Hashable,
    V: Data,
    R2: Diff,
    R1: Diff + Mul<R2>,
    <R1 as Mul<R2>>::Output: Diff
{
    arranged.join_core(other, |k,v,_| Some((k.clone(), v.clone())))
}

fn antijoin_arranged<G,K,V,R1,R2,T1,T2>(arranged: &Arranged<G, K, V, R1, T1>,
                                        other: &Arranged<G, K, (), R2, T2>) -> Collection<G, (K, V), R1>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    T1: TraceReader<K,V,G::Timestamp,R1> + Clone + 'static,
    T2: TraceReader<K,(),G::Timestamp,R2> + Clone + 'static,
    K: Data+Hashable,
    V: Data,
    R2: Diff,
    R1: Diff + Mul<R2, Output=R1>
{
    arranged.as_collection(|k,v|(k.clone(), v.clone())).concat(&semijoin_arranged(arranged, other).negate())
}


// TODO: remove when `fn concatenate()` in `collection.rs` makes it to a released version of DD
pub fn concatenate_collections<G, D, R, I>(scope: &mut G, iterator: I) -> Collection<G, D, R>
where
    G: Scope,
    D: Data,
    R: Monoid,
    I: IntoIterator<Item=Collection<G, D, R>>,
{
    scope
        .concatenate(iterator.into_iter().map(|x| x.inner))
        .as_collection()
}
