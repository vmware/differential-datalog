use timely::order::Product;
use timely::dataflow::scopes::{Child};
use timely::dataflow::operators::feedback::Handle;
use differential_dataflow::{Data, ExchangeData, Collection, Hashable};
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::difference::Semigroup;
use num::One;
use timely::dataflow::operators::*;
use timely::dataflow::*;

use crate::profile::*;
use crate::program::{TSNested, Weight};

/// A collection defined by multiple mutually recursive rules.
///
/// A `Variable` names a collection that may be used in mutually recursive rules. This implementation
/// is like the `Variable` defined in `iterate.rs` optimized for Datalog rules: it supports repeated
/// addition of collections, and a final `distinct` operator applied before connecting the definition.
pub struct Variable<'a, G: Scope, D: ExchangeData+Default+Data+Hashable>
where G::Timestamp: Lattice+Ord {
    feedback: Option<Handle<Child<'a, G, Product<G::Timestamp, TSNested>>, (D, Product<G::Timestamp, TSNested>, Weight)>>,
    current: Collection<Child<'a, G, Product<G::Timestamp, TSNested>>, D, Weight>,
    cycle: Collection<Child<'a, G, Product<G::Timestamp, TSNested>>, D, Weight>,
    name: String
}

impl<'a, G: Scope, D: ExchangeData+Default+Data+Hashable> Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    /// Creates a new `Variable` from a supplied `source` stream.
    /*pub fn from(source: &Collection<Child<'a, G, u64>, D>) -> Variable<'a, G, D> {
        let (feedback, cycle) = source.inner.scope().loop_variable(u64::max_value(), 1);
        let cycle = Collection::new(cycle);
        let mut result = Variable { feedback: Some(feedback), current: cycle.clone(), cycle: cycle };
        result.add(source);
        result
    }*/
    pub fn from(
        source: &Collection<Child<'a, G, Product<G::Timestamp, TSNested>>, D, Weight>,
        name: &str,
    ) -> Variable<'a, G, D> {
        let (feedback, cycle) = source.inner.scope().loop_variable(TSNested::one());
        let cycle_col = Collection::new(cycle);
        let mut result = Variable {
            feedback: Some(feedback),
            current: cycle_col.clone().filter(|_| false),
            cycle: cycle_col,
            name: name.to_string(),
        };
        result.add(source);
        result
    }

    /// Adds a new source of data to the `Variable`.
    pub fn add(
        &mut self,
        source: &Collection<Child<'a, G, Product<G::Timestamp, TSNested>>, D, Weight>,
    ) {
        self.current = self.current.concat(source);
    }
}

impl<'a, G: Scope, D: ExchangeData+Default+Data+Hashable> ::std::ops::Deref for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    type Target = Collection<Child<'a, G, Product<G::Timestamp,TSNested>>, D, Weight>;
    fn deref(&self) -> &Self::Target {
        &self.cycle
    }
}

impl<'a, G: Scope, D: ExchangeData+Default+Data+Hashable> Drop for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    fn drop(&mut self) {
        if let Some(feedback) = self.feedback.take() {
            with_prof_context(&format!("Variable: {}", self.name), || {
                self.current
                    .threshold(|_, c| if c.is_zero() { 0 } else { 1 })
                    .inner
                    .map(|(x, t, d)| (x, Product::new(t.outer, t.inner + TSNested::one()), d))
                    .connect_loop(feedback)
            });
        }
    }
}
