use fallible_iterator::FallibleIterator;
use differential_datalog::record::Record;

pub trait FromObservable<T, E> {
    fn from_observable(Observable<T, E>) -> Self;
}

pub trait IntoObservable<T, E> {
    fn into_observable(&self) -> Observable<T, E>;
}

// The consumer can subscribe to the channel
// which acts as an observable of deltas.
pub trait Observable<T, E> {
    fn subscribe(&self, observer: Observer<T, E>);
    // -> Subscription; TODO ignoring subscription for now
}

// The channel is an observer of changes from
// a producer
pub trait Observer<T, E>
{
    fn on_updates(&self, updates: FallibleIterator<Item = T, Error = E>);
    fn on_completed(&self);
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
pub trait Channel<T, E>: Observer<T, E> + Observable<T, E>{
}
