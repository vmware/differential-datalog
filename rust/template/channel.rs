use fallible_iterator::FallibleIterator;
use differential_datalog::record::Record;

// The consumer can subscribe to the channel
// which acts as an observable of deltas.
pub trait Observable<T, E>
{
    fn subscribe(&mut self, observer: Box<dyn Observer<T, E>>); // -> Subscription;
}

// The channel is an observer of changes from
// a producer
pub trait Observer<T, E>
// where I: Iterator<Item = T> // FallibleIterator<Item = T, Error = E>
{
    fn on_start(&self);
    fn on_commit(&self);
    fn on_updates<'a>(&self, updates: Box<dyn Iterator<Item = T> + 'a>);
    fn on_completed(self);
    // fn on_error(&self, error: ?);
}

// Stop listening to changes from the observable
pub trait Subscription
{
    fn unsubscribe(&self);
}

// A channel that transmits deltas. It acts as an observer of
// changes on the consuming end, and an observable on the
// producing end.
pub trait Channel<T, E>: Observer<T, E> + Observable<T, E>
{
}
