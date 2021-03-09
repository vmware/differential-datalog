use crate::{
    dataflow::{diff_distinct, FilterMap, MapExt},
    ddval::DDValue,
    program::arrange::Arrangement,
    render::{Offset, Str, TraceKey, TraceValue},
};
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{
        arrange::{Arrange, TraceAgent},
        ThresholdTotal,
    },
    trace::implementations::ord::OrdKeySpine,
    Collection, ExchangeData,
};
use std::ops::Add;
use timely::{dataflow::Scope, order::TotalOrder};

// TODO: Allow dynamic functions
pub type KeyFunc = fn(DDValue) -> Option<DDValue>;
pub type ValueFunc = fn(DDValue) -> Option<(DDValue, DDValue)>;

#[derive(Clone)]
pub struct ArrangeBy<'a> {
    pub kind: ArrangementKind,
    pub target_relation: Str<'a>,
    // TODO: Add source file location
}

#[derive(Clone)]
pub enum ArrangementKind {
    Set {
        /// The function for extracting the relation's key value,
        /// can be `None` if the value is already the key
        key_function: Option<KeyFunc>,
        /// Whether or not the set should be distinct
        distinct: bool,
    },
    Map {
        value_function: ValueFunc,
    },
}

impl ArrangementKind {
    pub const fn is_set(&self) -> bool {
        matches!(self, Self::Set { .. })
    }
}

type Arranged<S, R> =
    Arrangement<S, R, TraceAgent<TraceValue<S, R, Offset>>, TraceAgent<TraceKey<S, R, Offset>>>;

impl<'a> ArrangeBy<'a> {
    pub fn render<S, R>(&self, collection: &Collection<S, DDValue, R>) -> Arranged<S, R>
    where
        S: Scope,
        S::Timestamp: Lattice,
        R: Abelian + ExchangeData + Add<Output = R> + From<i8>,
    {
        // Name for the arrangement operation
        let arrangement_name = format!(
            "Arrange{}: {}",
            if self.kind.is_set() { "Set" } else { "Map" },
            self.target_relation,
        );

        match self.kind {
            ArrangementKind::Set {
                key_function,
                distinct,
            } => {
                // Arranges a collection by its key
                let arrange_set = |keyed: &Collection<S, (DDValue, ()), R>| {
                    let arranged =
                        keyed.arrange_named::<OrdKeySpine<_, _, _, Offset>>(&arrangement_name);

                    Arrangement::Set(arranged)
                };

                // The keyed relation
                let keyed = match self.key_collection(
                    collection,
                    key_function,
                    distinct,
                    &arrangement_name,
                ) {
                    Ok(keyed) => keyed,
                    Err(arranged) => return arranged,
                };

                if distinct {
                    // Our `diff_distinct()` impl returns an arrangement which is
                    // ready to be used as an arranged set
                    Arrangement::Set(diff_distinct(&*self.target_relation, &keyed))
                } else {
                    // The name mapping the collection from `key` to `(key, ())` so we can arrange it
                    let mapped_name = format!("Map: Map to key for {}", self.target_relation);
                    arrange_set(&keyed.map_named(&mapped_name, |key| (key, ())))
                }
            }

            ArrangementKind::Map { value_function } => {
                self.render_map(&collection, value_function, &arrangement_name)
            }
        }
    }

    pub fn render_root<S, R>(
        &self,
        collection: &Collection<S, DDValue, R>,
    ) -> Arrangement<S, R, TraceAgent<TraceValue<S, R, Offset>>, TraceAgent<TraceKey<S, R, Offset>>>
    where
        S: Scope,
        // TODO: The `TotalOrder` bound really puts a damper on
        //       code reuse, refactor things some more
        S::Timestamp: Lattice + TotalOrder,
        R: Abelian + ExchangeData + Add<Output = R> + From<i8>,
    {
        // Name for the arrangement operation
        let arrangement_name = format!(
            "Arrange{}: {}",
            if self.kind.is_set() { "Set" } else { "Map" },
            self.target_relation,
        );

        match self.kind {
            ArrangementKind::Set {
                key_function,
                distinct,
            } => {
                // Arranges a collection by its key
                let arrange_set = |keyed: &Collection<S, (DDValue, ()), R>| {
                    let arranged =
                        keyed.arrange_named::<OrdKeySpine<_, _, _, Offset>>(&arrangement_name);

                    Arrangement::Set(arranged)
                };

                // The keyed relation
                let mut keyed = match self.key_collection(
                    collection,
                    key_function,
                    distinct,
                    &arrangement_name,
                ) {
                    Ok(keyed) => keyed,
                    Err(arranged) => return arranged,
                };

                if distinct {
                    // Note: `Collection::threshold_total()` makes an arrangement, this just makes it explicit
                    //       and allows us to name it.
                    // TODO: Insert this arrangement into the collection of shared arrangements so
                    //       that we can try to re-use it if the opportunity arises
                    // TODO: Note that this `.arrange_named()` doesn't specify the `O` offset type.
                    //       this is because `.threshold_total()` only accepts traces that use `usize`
                    //       as the offset, whereas we'd prefer to use `Offset`
                    let threshold_arranged = keyed
                        .arrange_named::<OrdKeySpine<DDValue, S::Timestamp, R>>(&format!(
                            "ArrangeBySelf: ThresholdTotal for {}",
                            self.target_relation,
                        ));

                    // FIXME: If `diff` is negative, this will promote it to `1`
                    // TODO: `.threshold_total()` doesn't allow renaming the operator,
                    //       make a custom op or PR differential-dataflow
                    keyed = threshold_arranged.threshold_total(|_, diff| {
                        if diff.is_zero() {
                            R::from(0)
                        } else {
                            R::from(1)
                        }
                    });
                }

                // The name mapping the collection from `key` to `(key, ())` so we can arrange it
                let mapped_name = format!("Map: Map to key for {}", self.target_relation);
                arrange_set(&keyed.map_named(&mapped_name, |key| (key, ())))
            }

            ArrangementKind::Map { value_function } => {
                self.render_map(&collection, value_function, &arrangement_name)
            }
        }
    }

    fn render_map<S, R>(
        &self,
        collection: &Collection<S, DDValue, R>,
        value_function: ValueFunc,
        arrangement_name: &str,
    ) -> Arranged<S, R>
    where
        S: Scope,
        S::Timestamp: Lattice,
        R: Abelian + ExchangeData + Add<Output = R> + From<i8>,
    {
        // Extract the relation's key and value tuple before arranging it
        let arranged = collection
            .filter_map_named(
                &format!(
                    "FilterMap: Extract key and value for {}",
                    self.target_relation,
                ),
                value_function,
            )
            .arrange_named(arrangement_name);

        Arrangement::Map(arranged)
    }

    fn key_collection<S, R>(
        &self,
        collection: &Collection<S, DDValue, R>,
        key_function: Option<KeyFunc>,
        distinct: bool,
        arrangement_name: &str,
    ) -> Result<Collection<S, DDValue, R>, Arranged<S, R>>
    where
        S: Scope,
        S::Timestamp: Lattice,
        R: Abelian + ExchangeData + Add<Output = R> + From<i8>,
    {
        if let Some(key_function) = key_function {
            // The name for extracting the set's key out of the relation
            let keyed_name = format!("FilterMap: Extract key for {}", self.target_relation);

            if distinct {
                Ok(collection.filter_map_named(&keyed_name, key_function))

            // If our set is filtered and is not distinct we can skip a redundant map
            // operation by mapping into a `(key, ())` within the filter itself
            } else {
                let keyed = collection.filter_map_named(&keyed_name, move |value| {
                    key_function(value).map(|key| (key, ()))
                });

                let arranged =
                    keyed.arrange_named::<OrdKeySpine<_, _, _, Offset>>(&arrangement_name);

                Err(Arrangement::Set(arranged))
            }

        // If the collection has no key function then the current value is the key
        } else {
            Ok(collection.clone())
        }
    }
}
