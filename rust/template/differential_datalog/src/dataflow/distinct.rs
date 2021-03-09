use crate::dataflow::{ConsolidateExt, MapExt};
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{
        arrange::{ArrangeBySelf, Arranged, TraceAgent},
        Reduce,
    },
    trace::{implementations::ord::OrdKeySpine, layers::ordered::OrdOffset},
    Collection, ExchangeData, Hashable,
};
use std::{
    convert::{TryFrom, TryInto},
    fmt::Debug,
    ops::Add,
};
use timely::dataflow::Scope;

/// An alternative implementation of `distinct`.
///
/// The implementation of `distinct` in differential dataflow maintains both its input and output
/// arrangements.  This implementation, suggested by @frankmcsherry instead uses a single
/// arrangement that produces the number of "surplus" records, which are then subtracted from the
/// input to get an output with distinct records. This has the advantage that for keys that are
/// already distinct, there is no additional memory used in the output (nothing to subtract).  It
/// has the downside that if the input changes a lot, the output may have more changes (to track
/// the input changes) than if it just recorded distinct records (which is pretty stable).
pub fn diff_distinct<S, D, R, O>(
    relation_name: &str,
    collection: &Collection<S, D, R>,
) -> Arranged<S, TraceAgent<OrdKeySpine<D, S::Timestamp, R, O>>>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hashable,
    R: Abelian + ExchangeData + Add<Output = R> + From<i8>,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    // For each value with weight w != 1, compute an adjustment record with the same value and
    // weight (1-w)
    // TODO: What happens when negative weights get into this?
    let negated = collection
        .arrange_by_self_named(&format!(
            "ArrangeBySelf: DiffDistinct for {}",
            relation_name
        ))
        .reduce_named(
            &format!("Reduce: DiffDistinct for {}", relation_name),
            |_, src, dst| {
                // If the input weight is 1, don't produce a surplus record.
                if src[0].1 != R::from(1) {
                    dst.push(((), R::from(1) + src[0].1.clone().neg()))
                }
            },
        )
        .map_named(&format!("Map: DiffDistinct for {}", relation_name), |x| x.0);

    collection
        // TODO: `.concat_named()`?
        .concat(&negated)
        // We directly return the consolidation arrangement,
        // allowing us to potentially skip re-arranging it later
        .consolidate_arranged_named(&format!(
            "ConsolidateArranged: DiffDistinct for {}",
            relation_name,
        ))
}
