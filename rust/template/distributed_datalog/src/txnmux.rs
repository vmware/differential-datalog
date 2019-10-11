use std::any::Any;
use std::fmt::Debug;

use observe::CachingObserver;
use observe::Observable;
use observe::ObservableBox;
use observe::ObserverBox;
use observe::OptionalObserver;
use observe::SharedObserver;

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
            observer: SharedObserver::new(OptionalObserver::default()),
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
