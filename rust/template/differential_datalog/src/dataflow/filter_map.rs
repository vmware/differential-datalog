use differential_dataflow::{collection::AsCollection, difference::Semigroup, Collection};
use timely::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream},
    Data,
};

pub trait FilterMap<D, D2> {
    type Output;

    fn filter_map<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        self.filter_map_named("FilterMap", logic)
    }

    fn filter_map_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static;
}

impl<S, D, D2> FilterMap<D, D2> for Stream<S, D>
where
    S: Scope,
    D: Data,
    D2: Data,
{
    type Output = Stream<S, D2>;

    fn filter_map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        let mut buffer = Vec::new();

        self.unary(Pipeline, name, move |_capability, _info| {
            move |input, output| {
                input.for_each(|capability, data| {
                    data.swap(&mut buffer);

                    output
                        .session(&capability)
                        .give_iterator(buffer.drain(..).filter_map(|data| logic(data)));
                });
            }
        })
    }
}

impl<S, D, D2, R> FilterMap<D, D2> for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    D2: Data,
    R: Semigroup,
{
    type Output = Collection<S, D2, R>;

    fn filter_map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        self.inner
            .filter_map_named(name, move |(data, time, diff)| {
                logic(data).map(|data| (data, time, diff))
            })
            .as_collection()
    }
}
