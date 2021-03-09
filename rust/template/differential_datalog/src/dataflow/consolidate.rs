use differential_dataflow::{
    difference::Semigroup,
    lattice::Lattice,
    operators::arrange::{Arrange, Arranged, TraceAgent},
    trace::{implementations::ord::OrdKeySpine, layers::ordered::OrdOffset},
    Collection, ExchangeData, Hashable,
};
use std::{
    convert::{TryFrom, TryInto},
    fmt::Debug,
};
use timely::dataflow::Scope;

pub trait ConsolidateExt<S, D, R, O>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData,
    R: Semigroup + ExchangeData,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    /// A `.consolidate()` that returns its internal arrangement
    fn consolidate_arranged(&self) -> Arranged<S, TraceAgent<OrdKeySpine<D, S::Timestamp, R, O>>> {
        self.consolidate_arranged_named("ConsolidateArranged")
    }

    /// The same as `.consolidate_arranged()` but with the ability to name the operator.
    fn consolidate_arranged_named(
        &self,
        name: &str,
    ) -> Arranged<S, TraceAgent<OrdKeySpine<D, S::Timestamp, R, O>>>;
}

impl<S, D, R, O> ConsolidateExt<S, D, R, O> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hashable,
    R: Semigroup + ExchangeData,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn consolidate_arranged_named(
        &self,
        name: &str,
    ) -> Arranged<S, TraceAgent<OrdKeySpine<D, S::Timestamp, R, O>>> {
        // TODO: Name this map?
        self.map(|key| (key, ())).arrange_named(name)
    }
}
