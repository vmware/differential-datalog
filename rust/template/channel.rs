use differential_datalog::program::Response;
use differential_datalog::record::Record;
use std::sync::Arc;

// The consumer can subscribe to the channel
// which acts as an observable of deltas.
pub trait Observable<T, E>
{
    fn subscribe<'a>(&'a mut self, observer: Arc<dyn Observer<T, E>>) -> Box<dyn Subscription + 'a>;
}

// The channel is an observer of changes from
// a producer
pub trait Observer<T, E>
{
    fn on_start(&self) -> Response<()>;
    fn on_commit(&self) -> Response<()>;
    fn on_updates<'a>(&self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Response<()>;
    fn on_completed(self) -> Response<()>;
}

// Stop listening to changes from the observable
pub trait Subscription<'a> {
    fn unsubscribe(&'a mut self);
}

// A channel that transmits deltas. It acts as an observer of
// changes on the consuming end, and an observable on the
// producing end.
pub trait Channel<T, E>: Observer<T, E> + Observable<T, E> {}
