use std::any::Any;
use std::collections::LinkedList;
use std::fmt::Debug;
use std::mem::replace;

use crate::observe::Observable;
use crate::observe::ObservableBox;
use crate::observe::Observer;
use crate::observe::ObserverBox;
use crate::observe::OptionalObserver;
use crate::observe::SharedObserver;

/// Wrapper around a `SharedObserver` that stores updates and pushes them
/// forward only when an `on_commit` is received.
#[derive(Debug)]
struct CachingObserver<O, T> {
    /// The observer we ultimately push our data to when we received the
    /// `on_commit` event.
    observer: SharedObserver<OptionalObserver<O>>,
    /// The data we accumulated so far.
    data: Option<LinkedList<Vec<T>>>,
}

impl<O, T> CachingObserver<O, T> {
    /// Create a new `CachingObserver` wrapping the provided observer.
    pub fn new(observer: SharedObserver<OptionalObserver<O>>) -> Self {
        Self {
            observer,
            data: None,
        }
    }
}

impl<O, T, E> Observer<T, E> for CachingObserver<O, T>
where
    O: Observer<T, E>,
    T: Send + Debug,
    E: Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        if self.data.is_none() {
            self.data = Some(LinkedList::new());
        } else {
            panic!("received multiple on_start events")
        }
        Ok(())
    }

    fn on_commit(&mut self) -> Result<(), E> {
        if let Some(ref mut data) = self.data.take() {
            let updates = replace(data, LinkedList::new());
            let mut guard = self.observer.lock().unwrap();
            guard.on_start()?;
            guard.on_updates(Box::new(updates.into_iter().flatten()))?;
            guard.on_commit()?;
        } else {
            panic!("on_commit was not preceded by an on_start event")
        }
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        if let Some(ref mut data) = self.data {
            data.push_back(updates.collect());
        } else {
            panic!("on_updates was not preceded by an on_start event")
        }
        Ok(())
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.observer.on_completed()
    }
}

/// A multiplexer for transactions. In a nutshell, this is an object
/// that tracks a set of `Observable`s and implements the `Observable`
/// interface (i.e., can be subscribed to) itself. It will ensure that
/// the registered `Observable`'s transactions are serialized, meaning
/// the concurrent transactions will be submitted one after the other
/// without interleaving.
#[derive(Debug, Default)]
pub struct TxnMux<T, E>
where
    T: Debug + Send,
    E: Debug + Send,
{
    /// The observables we track and our subscriptions to them.
    subscriptions: Vec<(ObservableBox<T, E>, Box<dyn Any>)>,
    /// A reference to the `Observer` subscribed to us, if any.
    observer: SharedObserver<OptionalObserver<ObserverBox<T, E>>>,
}

impl<T, E> TxnMux<T, E>
where
    T: Debug + Send + 'static,
    E: Debug + Send + 'static,
{
    /// Create a new `TxnMux`, without any observables.
    pub fn new() -> Self {
        Self {
            subscriptions: Vec::new(),
            observer: SharedObserver::default(),
        }
    }

    /// Add an `Observable` to track to the multiplexer.
    pub fn add_observable(
        &mut self,
        mut observable: ObservableBox<T, E>,
    ) -> Result<(), ObservableBox<T, E>> {
        // Each observable gets its own `CachingObserver`, which will
        // take care of applying transactions in one go (serialized by
        // the shared observer's lock).
        let cacher = CachingObserver::new(self.observer.clone());
        match observable.subscribe_any(Box::new(cacher)) {
            Ok(subscription) => {
                self.subscriptions.push((observable, subscription));
                Ok(())
            }
            Err(_) => Err(observable),
        }
    }
}

impl<T, E> Drop for TxnMux<T, E>
where
    T: Debug + Send,
    E: Debug + Send,
{
    fn drop(&mut self) {
        for (mut observable, subscription) in self.subscriptions.drain(..).rev() {
            let _result = observable.unsubscribe_any(subscription.as_ref());
            debug_assert!(_result.is_some(), "{:?}", subscription);
        }
    }
}

impl<T, E> Observable<T, E> for TxnMux<T, E>
where
    T: Debug + Send + 'static,
    E: Debug + Send + 'static,
{
    type Subscription = ();

    fn subscribe(
        &mut self,
        observer: ObserverBox<T, E>,
    ) -> Result<Self::Subscription, ObserverBox<T, E>> {
        let mut guard = self.observer.lock().unwrap();
        if guard.is_some() {
            Err(observer)
        } else {
            guard.replace(observer);
            Ok(())
        }
    }

    fn unsubscribe(&mut self, _subscription: &Self::Subscription) -> Option<ObserverBox<T, E>> {
        self.observer.lock().unwrap().take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::observe::MockObserver;

    /// Test caching of transactions via a `CachingObserver`.
    #[test]
    fn transaction_caching() {
        let mock = SharedObserver::new(OptionalObserver::new(MockObserver::new()));
        let observer = &mut CachingObserver::new(mock.clone()) as &mut dyn Observer<_, ()>;

        assert_eq!(observer.on_start(), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_start, 0);

        assert_eq!(observer.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_updates, 0);

        assert_eq!(observer.on_updates(Box::new([6, 4, 5].iter())), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_updates, 0);

        assert_eq!(observer.on_commit(), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_start, 1);
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_updates, 6);
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_commit, 1);
    }
}
