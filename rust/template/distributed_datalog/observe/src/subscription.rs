use std::sync::Arc;
use std::sync::Mutex;

use crate::observer::ObserverBox;

/// A trait representing an object subscribed to an `Observable`.
pub trait Subscription {
    /// Cancel the subscription so the corresponding Observer
    /// stops listening to the corresponding Observable.
    fn unsubscribe(self: Box<Self>);
}

/// A trivial implementation of a `Subscription`.
#[derive(Debug)]
pub struct UpdatesSubscription<T, E> {
    /// A reference to the `Observer` we subscribed to.
    pub observer: Arc<Mutex<Option<ObserverBox<T, E>>>>,
}

impl<T, E> Subscription for UpdatesSubscription<T, E> {
    /// Cancel a subscription so that the observer stops listening to
    /// the observable.
    fn unsubscribe(self: Box<Self>) {
        *self.observer.lock().unwrap() = None;
    }
}
