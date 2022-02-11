use differential_dataflow::{difference::Semigroup, AsCollection, Collection};
use timely::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream},
    Data,
};

pub trait MapExt<D1, D2> {
    type Output;

    fn map_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        D1: Data,
        D2: Data,
        L: FnMut(D1) -> D2 + 'static;
}

impl<S, D1, D2> MapExt<D1, D2> for Stream<S, D1>
where
    S: Scope,
    D1: Data,
    D2: Data,
{
    type Output = Stream<S, D2>;

    #[allow(clippy::redundant_closure)]
    fn map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D1) -> D2 + 'static,
    {
        let mut buffer = Vec::new();

        self.unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut buffer);

                    output
                        .session(&time)
                        .give_iterator(buffer.drain(..).map(|x| logic(x)));
                });
            }
        })
    }
}

impl<S, D1, D2, R> MapExt<D1, D2> for Collection<S, D1, R>
where
    S: Scope,
    D1: Data,
    D2: Data,
    R: Semigroup,
{
    type Output = Collection<S, D2, R>;

    fn map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D1) -> D2 + 'static,
    {
        self.inner
            .map_named(name, move |(data, time, diff)| (logic(data), time, diff))
            .as_collection()
    }
}
