use timely::progress::nested::product::Product;
use timely::dataflow::*;
use timely::dataflow::scopes::{Child};
use timely::dataflow::operators::*;
use timely::dataflow::operators::feedback::Handle;
use differential_dataflow::{Data, Collection, Hashable};
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use profile::*;

/// A collection defined by multiple mutually recursive rules.
///
/// A `Variable` names a collection that may be used in mutually recursive rules. This implementation
/// is like the `Variable` defined in `iterate.rs` optimized for Datalog rules: it supports repeated
/// addition of collections, and a final `distinct` operator applied before connecting the definition.
pub struct Variable<'a, G: Scope, D: Default+Data+Hashable>
where G::Timestamp: Lattice+Ord {
    feedback: Option<Handle<G::Timestamp, u32,(D, Product<G::Timestamp, u32>, isize)>>,
    current: Collection<Child<'a, G, u32>, D>,
    cycle: Collection<Child<'a, G, u32>, D>,
    name: String
}

impl<'a, G: Scope, D: Default+Data+Hashable> Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    /// Creates a new `Variable` from a supplied `source` stream.
    /*pub fn from(source: &Collection<Child<'a, G, u64>, D>) -> Variable<'a, G, D> {
        let (feedback, cycle) = source.inner.scope().loop_variable(u64::max_value(), 1);
        let cycle = Collection::new(cycle);
        let mut result = Variable { feedback: Some(feedback), current: cycle.clone(), cycle: cycle };
        result.add(source);
        result
    }*/
    pub fn from(source: &Collection<Child<'a, G, u32>, D>, name: &str) -> Variable<'a, G, D> {
        let (feedback, cycle) = source.inner.scope().loop_variable(u32::max_value(), 1);
        let cycle = Collection::new(cycle);
        let mut result = Variable { feedback: Some(feedback), current: cycle.clone().filter(|_| false), cycle: cycle, name: name.to_string() };
        result.add(source);
        result
    }

    /// Adds a new source of data to the `Variable`.
    pub fn add(&mut self, source: &Collection<Child<'a, G, u32>, D>) {
        self.current = self.current.concat(source);
    }
}

impl<'a, G: Scope, D: Default+Data+Hashable> ::std::ops::Deref for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    type Target = Collection<Child<'a, G, u32>, D>;
    fn deref(&self) -> &Self::Target {
        &self.cycle
    }
}

impl<'a, G: Scope, D: Default+Data+Hashable> Drop for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    fn drop(&mut self) {
        if let Some(feedback) = self.feedback.take() {
            with_prof_context(
                &self.name,
                ||self.current.distinct()
                            .inner
                            .map(|(x,t,d)| (x, Product::new(t.outer, t.inner+1), d))
                            .connect_loop(feedback));
        }
    }
}

