//! Relation arrangements and transformations

use crate::{
    ddval::DDValue,
    program::{ArrId, TKeyAgent, TKeyEnter, TValAgent, TValEnter, Weight},
};
use differential_dataflow::{
    difference::{Diff, Monoid},
    hashable::Hashable,
    lattice::Lattice,
    operators::{
        arrange::arrangement::{ArrangeBySelf, Arranged},
        Consolidate, JoinCore, Reduce,
    },
    trace::{wrappers::enter::TraceEnter, BatchReader, Cursor, TraceReader},
    Collection, Data, ExchangeData,
};
use fnv::FnvHashMap;
use std::ops::{Add, Mul, Neg};
use timely::{
    dataflow::scopes::{Child, Scope},
    progress::{timestamp::Refines, Timestamp},
};

/// An arrangement originating either in the current scope or a higher one
#[derive(Clone)]
pub enum ArrangementFlavor<S, T>
where
    S: Scope,
    S::Timestamp: Lattice + Refines<T>,
    T: Lattice + Timestamp,
{
    /// An arrangement created within the current scope, usually created by
    /// [`Arrangement::enter_region()`] or made directly from the output of
    /// [`ArrangeBySelf::arrange_by_self()`] or similar methods
    Local(Arrangement<S, Weight, TValAgent<S::Timestamp>, TKeyAgent<S::Timestamp>>),
    /// An arrangement imported from a higher scope, usually created by [`Arrangement::enter()`]
    Foreign(Arrangement<S, Weight, TValEnter<T, S::Timestamp>, TKeyEnter<T, S::Timestamp>>),
}

/// A single [arrangement](Arranged), either arranged as a map of keys to values or as a set of keys
#[derive(Clone)]
pub enum Arrangement<S, R, Map, Set>
where
    S: Scope,
    S::Timestamp: Lattice,
    Map: TraceReader<Key = DDValue, Val = DDValue, Time = S::Timestamp, R = R> + Clone + 'static,
    Map::Batch: BatchReader<Map::Key, Map::Val, Map::Time, Map::R> + Clone + 'static,
    Map::Cursor: Cursor<Map::Key, Map::Val, Map::Time, Map::R>,
    Set: TraceReader<Key = DDValue, Val = (), Time = S::Timestamp, R = R> + Clone + 'static,
    Set::Batch: BatchReader<Set::Key, Set::Val, Set::Time, Set::R> + Clone + 'static,
    Set::Cursor: Cursor<Set::Key, Set::Val, Set::Time, Set::R>,
{
    /// An [arrangement](Arranged) of keys to associated values
    Map(Arranged<S, Map>),
    /// An [arrangement](Arranged) of keys
    Set(Arranged<S, Set>),
}

impl<S, R, Map, Set> Arrangement<S, R, Map, Set>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    Map: TraceReader<Key = DDValue, Val = DDValue, Time = S::Timestamp, R = R> + Clone + 'static,
    Map::Batch: BatchReader<Map::Key, Map::Val, Map::Time, Map::R> + Clone + 'static,
    Map::Cursor: Cursor<Map::Key, Map::Val, Map::Time, Map::R>,
    Set: TraceReader<Key = DDValue, Val = (), Time = S::Timestamp, R = R> + Clone + 'static,
    Set::Batch: BatchReader<Set::Key, Set::Val, Set::Time, Set::R> + Clone + 'static,
    Set::Cursor: Cursor<Set::Key, Set::Val, Set::Time, Set::R>,
{
    /// Brings an arranged collection out of a nested scope, see [`Arranged::enter()`]
    pub fn enter<'a, TInner>(
        &self,
        inner: &Child<'a, S, TInner>,
    ) -> Arrangement<Child<'a, S, TInner>, R, TraceEnter<Map, TInner>, TraceEnter<Set, TInner>>
    where
        R: 'static,
        TInner: Refines<S::Timestamp> + Lattice + Timestamp + Clone + 'static,
    {
        match self {
            Self::Map(arr) => Arrangement::Map(arr.enter(inner)),
            Self::Set(arr) => Arrangement::Set(arr.enter(inner)),
        }
    }

    /// Brings an arranged collection into a nested region, see [`Arranged::enter_region()`]
    pub fn enter_region<'a>(
        &self,
        region: &Child<'a, S, S::Timestamp>,
    ) -> Arrangement<Child<'a, S, S::Timestamp>, R, Map, Set>
    where
        R: 'static,
    {
        match self {
            Self::Map(arr) => Arrangement::Map(arr.enter_region(region)),
            Self::Set(arr) => Arrangement::Set(arr.enter_region(region)),
        }
    }
}

impl<'a, S, R, Map, Set> Arrangement<Child<'a, S, S::Timestamp>, R, Map, Set>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    Map: TraceReader<Key = DDValue, Val = DDValue, Time = S::Timestamp, R = R> + Clone + 'static,
    Map::Batch: BatchReader<Map::Key, Map::Val, Map::Time, Map::R> + Clone + 'static,
    Map::Cursor: Cursor<Map::Key, Map::Val, Map::Time, Map::R>,
    Set: TraceReader<Key = DDValue, Val = (), Time = S::Timestamp, R = R> + Clone + 'static,
    Set::Batch: BatchReader<Set::Key, Set::Val, Set::Time, Set::R> + Clone + 'static,
    Set::Cursor: Cursor<Set::Key, Set::Val, Set::Time, Set::R>,
{
    /// Brings an arranged collection out of a nested region, see [`Arranged::leave_region()`]
    pub fn leave_region(&self) -> Arrangement<S, R, Map, Set> {
        match self {
            Self::Map(arr) => Arrangement::Map(arr.leave_region()),
            Self::Set(arr) => Arrangement::Set(arr.leave_region()),
        }
    }
}

pub(super) struct Arrangements<'a, S, T>
where
    S: Scope,
    S::Timestamp: Lattice + Refines<T>,
    T: Lattice + Timestamp,
{
    pub(super) arrangements: &'a FnvHashMap<ArrId, ArrangementFlavor<S, T>>,
}

impl<'a, S, T> Arrangements<'a, S, T>
where
    S: Scope,
    S::Timestamp: Lattice + Refines<T>,
    T: Lattice + Timestamp,
{
    pub(super) fn lookup_arr(&self, arrid: ArrId) -> ArrangementFlavor<S, T> {
        self.arrangements
            .get(&arrid)
            .cloned()
            .unwrap_or_else(|| panic!("mk_rule: unknown arrangement {:?}", arrid))
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

/// An alternative implementation of `distinct`.
///
/// The implementation of `distinct` in differential dataflow maintains both its input and output
/// arrangements.  This implementation, suggested by @frankmcsherry instead uses a single
/// arrangement that produces the number of "surplus" records, which are then subtracted from the
/// input to get an output with distinct records. This has the advantage that for keys that are
/// already distinct, there is no additional memory used in the output (nothing to subtract).  It
/// has the downside that if the input changes a lot, the output may have more changes (to track
/// the input changes) than if it just recorded distinct records (which is pretty stable).
pub fn diff_distinct<G, D, R>(collection: &Collection<G, D, R>) -> Collection<G, D, R>
where
    G: Scope,
    G::Timestamp: Lattice,
    D: ExchangeData + Hashable,
    R: Monoid + ExchangeData + Neg<Output = R> + Add<R, Output = R> + From<i8>,
{
    collection
        .concat(
            // For each value with weight w != 1, compute an adjustment record with the same value and
            // weight (1-w)
            &collection
                .arrange_by_self()
                .reduce(|_, src, dst| {
                    // If the input weight is 1, don't produce a surplus record.
                    if src[0].1 != R::from(1) {
                        dst.push(((), R::from(1) + src[0].1.clone().neg()))
                    }
                })
                .map(|x| x.0),
        )
        .consolidate()
}
