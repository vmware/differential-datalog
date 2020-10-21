//! Relation arrangements and transformations

use crate::{
    ddval::DDValue,
    program::{ArrId, Dep, TKeyAgent, TKeyEnter, TSNested, TValAgent, TValEnter, TupleTS, Weight},
};
use differential_dataflow::{
    difference::{Diff, Monoid},
    hashable::Hashable,
    lattice::Lattice,
    operators::{
        arrange::{arrangement::Arranged, Arrange},
        JoinCore, Threshold, ThresholdTotal,
    },
    trace::{BatchReader, Cursor, TraceReader},
    AsCollection, Collection, Data,
};
use fnv::{FnvHashMap, FnvHashSet};
use num::Zero;
use std::ops::Mul;
use timely::{
    dataflow::{
        operators::Concatenate,
        scopes::{Child, Scope, ScopeParent},
    },
    order::{Product, TotalOrder},
    progress::{timestamp::Refines, Timestamp},
};

/// Function type used to map the content of a relation
/// (see `XFormCollection::Map`).
pub type MapFunc = fn(DDValue) -> DDValue;

/// (see `XFormCollection::FlatMap`).
pub type FlatMapFunc = fn(DDValue) -> Option<Box<dyn Iterator<Item = DDValue>>>;

/// Function type used to filter a relation
/// (see `XForm*::Filter`).
pub type FilterFunc = fn(&DDValue) -> bool;

/// Function type used to simultaneously filter and map a relation
/// (see `XFormCollection::FilterMap`).
pub type FilterMapFunc = fn(DDValue) -> Option<DDValue>;

/// Function type used to inspect a relation
/// (see `XFormCollection::InspectFunc`)
pub type InspectFunc = fn(&DDValue, TupleTS, Weight) -> ();

/// Function type used to arrange a relation into key-value pairs
/// (see `XFormArrangement::Join`, `XFormArrangement::Antijoin`).
pub type ArrangeFunc = fn(DDValue) -> Option<(DDValue, DDValue)>;

/// Function type used to assemble the result of a join into a value.
/// Takes join key and a pair of values from the two joined relation
/// (see `XFormArrangement::Join`).
pub type JoinFunc = fn(&DDValue, &DDValue, &DDValue) -> Option<DDValue>;

/// Function type used to assemble the result of a semijoin into a value.
/// Takes join key and value (see `XFormArrangement::Semijoin`).
pub type SemijoinFunc = fn(&DDValue, &DDValue, &()) -> Option<DDValue>;

/// Aggregation function: aggregates multiple values into a single value.
pub type AggFunc = fn(&DDValue, &[(&DDValue, Weight)]) -> Option<DDValue>;

/// Describes arrangement of a relation.
#[derive(Clone)]
pub enum Arrangement {
    /// Arrange into (key,value) pairs
    Map {
        /// Arrangement name; does not have to be unique
        name: String,
        /// Function used to produce arrangement.
        afun: &'static ArrangeFunc,
        /// The arrangement can be queried using `RunningProgram::query_arrangement`
        /// and `RunningProgram::dump_arrangement`.
        queryable: bool,
    },
    /// Arrange into a set of values
    Set {
        /// Arrangement name; does not have to be unique
        name: String,
        /// Function used to produce arrangement.
        fmfun: &'static FilterMapFunc,
        /// Apply distinct_total() before arranging filtered collection.
        /// This is necessary if the arrangement is to be used in an antijoin.
        distinct: bool,
    },
}

impl Arrangement {
    pub(super) fn name(&self) -> String {
        match self {
            Arrangement::Map { name, .. } => name.clone(),
            Arrangement::Set { name, .. } => name.clone(),
        }
    }

    pub(super) fn queryable(&self) -> bool {
        match self {
            Arrangement::Map { queryable, .. } => *queryable,
            Arrangement::Set { .. } => false,
        }
    }

    pub(super) fn build_arrangement_root<S>(
        &self,
        collection: &Collection<S, DDValue, Weight>,
    ) -> ArrangedCollection<S, TValAgent<S>, TKeyAgent<S>>
    where
        S: Scope,
        Collection<S, DDValue, Weight>: ThresholdTotal<S, DDValue, Weight>,
        S::Timestamp: Lattice + Ord + TotalOrder,
    {
        match self {
            Arrangement::Map { afun, .. } => {
                ArrangedCollection::Map(collection.flat_map(*afun).arrange())
            }
            Arrangement::Set {
                fmfun, distinct, ..
            } => {
                let filtered = collection.flat_map(*fmfun);
                if *distinct {
                    ArrangedCollection::Set(
                        filtered
                            .threshold_total(|_, c| if c.is_zero() { 0 } else { 1 })
                            .map(|k| (k, ()))
                            .arrange(), /* arrange_by_self() */
                    )
                } else {
                    ArrangedCollection::Set(filtered.map(|k| (k, ())).arrange())
                }
            }
        }
    }

    pub(super) fn build_arrangement<S>(
        &self,
        collection: &Collection<S, DDValue, Weight>,
    ) -> ArrangedCollection<S, TValAgent<S>, TKeyAgent<S>>
    where
        S: Scope,
        S::Timestamp: Lattice + Ord,
    {
        match self {
            Arrangement::Map { afun, .. } => {
                ArrangedCollection::Map(collection.flat_map(*afun).arrange())
            }
            Arrangement::Set {
                fmfun, distinct, ..
            } => {
                let filtered = collection.flat_map(*fmfun);
                if *distinct {
                    ArrangedCollection::Set(
                        filtered
                            .threshold(|_, c| if c.is_zero() { 0 } else { 1 })
                            .map(|k| (k, ()))
                            .arrange(),
                    )
                } else {
                    ArrangedCollection::Set(filtered.map(|k| (k, ())).arrange())
                }
            }
        }
    }
}

pub(super) enum ArrangedCollection<S, T1, T2>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    T1: TraceReader<Key = DDValue, Val = DDValue, Time = S::Timestamp, R = Weight> + Clone,
    T1::Batch: BatchReader<DDValue, DDValue, S::Timestamp, Weight>,
    T1::Cursor: Cursor<DDValue, DDValue, S::Timestamp, Weight>,
    T2: TraceReader<Key = DDValue, Val = (), Time = S::Timestamp, R = Weight> + Clone,
    T2::Batch: BatchReader<DDValue, (), S::Timestamp, Weight>,
    T2::Cursor: Cursor<DDValue, (), S::Timestamp, Weight>,
{
    Map(Arranged<S, T1>),
    Set(Arranged<S, T2>),
}

impl<S> ArrangedCollection<S, TValAgent<S>, TKeyAgent<S>>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
{
    pub(super) fn enter<'a>(
        &self,
        inner: &Child<'a, S, Product<S::Timestamp, TSNested>>,
    ) -> ArrangedCollection<
        Child<'a, S, Product<S::Timestamp, TSNested>>,
        TValEnter<S, Product<S::Timestamp, TSNested>>,
        TKeyEnter<S, Product<S::Timestamp, TSNested>>,
    > {
        match self {
            ArrangedCollection::Map(arr) => ArrangedCollection::Map(arr.enter(inner)),
            ArrangedCollection::Set(arr) => ArrangedCollection::Set(arr.enter(inner)),
        }
    }
}

/// Helper type that represents an arranged collection of one of two
/// types (e.g., an arrangement created in a local scope or entered from
/// the parent scope)
pub(super) enum A<'a, 'b, P, T>
where
    P: ScopeParent,
    P::Timestamp: Lattice + Ord,
    T: Refines<P::Timestamp> + Lattice + Timestamp + Ord,
    'a: 'b,
{
    Arrangement1(
        &'b ArrangedCollection<
            Child<'a, P, T>,
            TValAgent<Child<'a, P, T>>,
            TKeyAgent<Child<'a, P, T>>,
        >,
    ),
    Arrangement2(&'b ArrangedCollection<Child<'a, P, T>, TValEnter<'a, P, T>, TKeyEnter<'a, P, T>>),
}

pub(super) struct Arrangements<'a, 'b, P, T>
where
    P: ScopeParent,
    P::Timestamp: Lattice + Ord,
    T: Refines<P::Timestamp> + Lattice + Timestamp + Ord,
    'a: 'b,
{
    pub(super) arrangements1: &'b FnvHashMap<
        ArrId,
        ArrangedCollection<Child<'a, P, T>, TValAgent<Child<'a, P, T>>, TKeyAgent<Child<'a, P, T>>>,
    >,
    pub(super) arrangements2: &'b FnvHashMap<
        ArrId,
        ArrangedCollection<Child<'a, P, T>, TValEnter<'a, P, T>, TKeyEnter<'a, P, T>>,
    >,
}

impl<'a, 'b, P, T> Arrangements<'a, 'b, P, T>
where
    P: ScopeParent,
    P::Timestamp: Lattice + Ord,
    T: Refines<P::Timestamp> + Lattice + Timestamp + Ord,
    'a: 'b,
{
    pub(super) fn lookup_arr(&self, arrid: ArrId) -> A<'a, 'b, P, T> {
        self.arrangements1.get(&arrid).map_or_else(
            || {
                self.arrangements2
                    .get(&arrid)
                    .map(|arr| A::Arrangement2(arr))
                    .unwrap_or_else(|| panic!("mk_rule: unknown arrangement {:?}", arrid))
            },
            |arr| A::Arrangement1(arr),
        )
    }
}

/// Transformations, such as maps, flatmaps, filters, joins, etc. are the building blocks of
/// DDlog rules.
///
/// Different kinds of transformations can be applied only to flat collections,
/// only to arranged collections, or both. We therefore use separate types to represent
/// collection and arrangement transformations.
///
/// Note that differential sometimes allows the same kind of transformation to be applied to both
/// collections and arrangements; however the former is implemented on top of the latter and incurs
/// the additional cost of arranging the collection. We only support the arranged version of these
/// transformations, forcing the user to explicitly arrange the collection if necessary (or, as much
/// as possible, keep the data arranged throughout the chain of transformations).
///
/// `XFormArrangement` - arrangement transformation.
#[derive(Clone)]
pub enum XFormArrangement {
    /// FlatMap arrangement into a collection
    FlatMap {
        description: String,
        fmfun: &'static FlatMapFunc,
        /// Transformation to apply to resulting collection.
        /// `None` terminates the chain of transformations.
        next: Box<Option<XFormCollection>>,
    },
    FilterMap {
        description: String,
        fmfun: &'static FilterMapFunc,
        /// Transformation to apply to resulting collection.
        /// `None` terminates the chain of transformations.
        next: Box<Option<XFormCollection>>,
    },
    /// Aggregate
    Aggregate {
        description: String,
        /// Filter arrangement before grouping
        ffun: Option<&'static FilterFunc>,
        /// Aggregation to apply to each group.
        aggfun: &'static AggFunc,
        /// Apply transformation to the resulting collection.
        next: Box<Option<XFormCollection>>,
    },
    /// Join
    Join {
        description: String,
        /// Filter arrangement before joining
        ffun: Option<&'static FilterFunc>,
        /// Arrangement to join with.
        arrangement: ArrId,
        /// Function used to put together ouput value.
        jfun: &'static JoinFunc,
        /// Join returns a collection: apply `next` transformation to it.
        next: Box<Option<XFormCollection>>,
    },
    /// Semijoin
    Semijoin {
        description: String,
        /// Filter arrangement before joining
        ffun: Option<&'static FilterFunc>,
        /// Arrangement to semijoin with.
        arrangement: ArrId,
        /// Function used to put together ouput value.
        jfun: &'static SemijoinFunc,
        /// Join returns a collection: apply `next` transformation to it.
        next: Box<Option<XFormCollection>>,
    },
    /// Return a subset of values that correspond to keys not present in `arrangement`.
    Antijoin {
        description: String,
        /// Filter arrangement before joining
        ffun: Option<&'static FilterFunc>,
        /// Arrangement to antijoin with
        arrangement: ArrId,
        /// Antijoin returns a collection: apply `next` transformation to it.
        next: Box<Option<XFormCollection>>,
    },
}

impl XFormArrangement {
    pub fn description(&self) -> &str {
        match self {
            XFormArrangement::FlatMap { description, .. } => &description,
            XFormArrangement::FilterMap { description, .. } => &description,
            XFormArrangement::Aggregate { description, .. } => &description,
            XFormArrangement::Join { description, .. } => &description,
            XFormArrangement::Semijoin { description, .. } => &description,
            XFormArrangement::Antijoin { description, .. } => &description,
        }
    }

    pub(super) fn dependencies(&self) -> FnvHashSet<Dep> {
        match self {
            XFormArrangement::FlatMap { next, .. } => match **next {
                None => FnvHashSet::default(),
                Some(ref n) => n.dependencies(),
            },
            XFormArrangement::FilterMap { next, .. } => match **next {
                None => FnvHashSet::default(),
                Some(ref n) => n.dependencies(),
            },
            XFormArrangement::Aggregate { next, .. } => match **next {
                None => FnvHashSet::default(),
                Some(ref n) => n.dependencies(),
            },
            XFormArrangement::Join {
                arrangement, next, ..
            } => {
                let mut deps = match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies(),
                };
                deps.insert(Dep::Arr(*arrangement));
                deps
            }
            XFormArrangement::Semijoin {
                arrangement, next, ..
            } => {
                let mut deps = match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies(),
                };
                deps.insert(Dep::Arr(*arrangement));
                deps
            }
            XFormArrangement::Antijoin {
                arrangement, next, ..
            } => {
                let mut deps = match **next {
                    None => FnvHashSet::default(),
                    Some(ref n) => n.dependencies(),
                };
                deps.insert(Dep::Arr(*arrangement));
                deps
            }
        }
    }
}

/// `XFormCollection` - collection transformation.
#[derive(Clone)]
pub enum XFormCollection {
    /// Arrange the collection, apply `next` transformation to the resulting collection.
    Arrange {
        description: String,
        afun: &'static ArrangeFunc,
        next: Box<XFormArrangement>,
    },
    /// Apply `mfun` to each element in the collection
    Map {
        description: String,
        mfun: &'static MapFunc,
        next: Box<Option<XFormCollection>>,
    },
    /// FlatMap
    FlatMap {
        description: String,
        fmfun: &'static FlatMapFunc,
        next: Box<Option<XFormCollection>>,
    },
    /// Filter collection
    Filter {
        description: String,
        ffun: &'static FilterFunc,
        next: Box<Option<XFormCollection>>,
    },
    /// Map and filter
    FilterMap {
        description: String,
        fmfun: &'static FilterMapFunc,
        next: Box<Option<XFormCollection>>,
    },
    /// Inspector
    Inspect {
        description: String,
        ifun: &'static InspectFunc,
        next: Box<Option<XFormCollection>>,
    },
}

impl XFormCollection {
    pub fn description(&self) -> &str {
        match self {
            XFormCollection::Arrange { description, .. } => &description,
            XFormCollection::Map { description, .. } => &description,
            XFormCollection::FlatMap { description, .. } => &description,
            XFormCollection::Filter { description, .. } => &description,
            XFormCollection::FilterMap { description, .. } => &description,
            XFormCollection::Inspect { description, .. } => &description,
        }
    }

    pub fn dependencies(&self) -> FnvHashSet<Dep> {
        match self {
            XFormCollection::Arrange { next, .. } => next.dependencies(),
            XFormCollection::Map { next, .. } => match **next {
                None => FnvHashSet::default(),
                Some(ref n) => n.dependencies(),
            },
            XFormCollection::FlatMap { next, .. } => match **next {
                None => FnvHashSet::default(),
                Some(ref n) => n.dependencies(),
            },
            XFormCollection::Filter { next, .. } => match **next {
                None => FnvHashSet::default(),
                Some(ref n) => n.dependencies(),
            },
            XFormCollection::FilterMap { next, .. } => match **next {
                None => FnvHashSet::default(),
                Some(ref n) => n.dependencies(),
            },
            XFormCollection::Inspect { next, .. } => match **next {
                None => FnvHashSet::default(),
                Some(ref n) => n.dependencies(),
            },
        }
    }
}

// Versions of semijoin and antijoin operators that take arrangement instead of collection.
fn semijoin_arranged<G, K, V, R1, R2, T1, T2>(
    arranged: &Arranged<G, T1>,
    other: &Arranged<G, T2>,
) -> Collection<G, (K, V), <R1 as Mul<R2>>::Output>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
    T1: TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R1> + Clone + 'static,
    T1::Batch: BatchReader<K, V, G::Timestamp, R1>,
    T1::Cursor: Cursor<K, V, G::Timestamp, R1>,
    T2: TraceReader<Key = K, Val = (), Time = G::Timestamp, R = R2> + Clone + 'static,
    T2::Batch: BatchReader<K, (), G::Timestamp, R2>,
    T2::Cursor: Cursor<K, (), G::Timestamp, R2>,
    K: Data + Hashable,
    V: Data,
    R2: Diff,
    R1: Diff + Mul<R2>,
    <R1 as Mul<R2>>::Output: Diff,
{
    arranged.join_core(other, |k, v, _| Some((k.clone(), v.clone())))
}

pub(super) fn antijoin_arranged<G, K, V, R1, R2, T1, T2>(
    arranged: &Arranged<G, T1>,
    other: &Arranged<G, T2>,
) -> Collection<G, (K, V), R1>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
    T1: TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R1> + Clone + 'static,
    T1::Batch: BatchReader<K, V, G::Timestamp, R1>,
    T1::Cursor: Cursor<K, V, G::Timestamp, R1>,
    T2: TraceReader<Key = K, Val = (), Time = G::Timestamp, R = R2> + Clone + 'static,
    T2::Batch: BatchReader<K, (), G::Timestamp, R2>,
    T2::Cursor: Cursor<K, (), G::Timestamp, R2>,
    K: Data + Hashable,
    V: Data,
    R2: Diff,
    R1: Diff + Mul<R2, Output = R1>,
{
    arranged
        .as_collection(|k, v| (k.clone(), v.clone()))
        .concat(&semijoin_arranged(arranged, other).negate())
}

// TODO: remove when `fn concatenate()` in `collection.rs` makes it to a released version of DD
pub fn concatenate_collections<G, D, R, I>(scope: &mut G, iterator: I) -> Collection<G, D, R>
where
    G: Scope,
    D: Data,
    R: Monoid,
    I: IntoIterator<Item = Collection<G, D, R>>,
{
    scope
        .concatenate(iterator.into_iter().map(|x| x.inner))
        .as_collection()
}
