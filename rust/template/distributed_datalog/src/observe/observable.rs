use std::any::Any;
use std::fmt::Debug;

use crate::observe::observer::ObserverBox;
use crate::observe::observer::OptionalObserver;
use crate::observe::observer::SharedObserver;

/// A boxed up `ObservableAny`.
pub type ObservableBox<T, E> = Box<dyn ObservableAny<T, E> + Send>;

/// A trait for objects that can be observed.
pub trait Observable<T, E>: Debug
where
    T: Send,
    E: Send,
{
    /// A subscription handed out as part of a `subscribe` that can
    /// later be used to unsubscribe an `Observer`, if desired.
    type Subscription;

    /// An Observer subscribes to an Observable to listen to data
    /// emitted by the latter. The Observer stops listening when the
    /// Subscription returned is canceled.
    ///
    /// A return value of `None` indicates that the `Observable` is
    /// unable to accept any more `Observer`s.
    fn subscribe(
        &mut self,
        observer: ObserverBox<T, E>,
    ) -> Result<Self::Subscription, ObserverBox<T, E>>;

    /// Cancel a subscription so that the observer stops listening to
    /// the observable.
    fn unsubscribe(&mut self, subscription: &Self::Subscription) -> Option<ObserverBox<T, E>>;
}

/// A trait abstracting away from the concrete type of subscription used
/// by an `Observable`.
pub trait ObservableAny<T, E>: Debug
where
    T: Send,
    E: Send,
{
    /// Subscribe an `Observer` to this `Observable`.
    fn subscribe_any(
        &mut self,
        observer: ObserverBox<T, E>,
    ) -> Result<Box<dyn Any + Send>, ObserverBox<T, E>>;

    /// Unsubscribe an `Observer` from this `Observable` using a
    /// previously handed out subscription.
    fn unsubscribe_any(&mut self, subscription: &dyn Any) -> Option<ObserverBox<T, E>>;
}

/// Any `Observable` whose subscription type implements `Any` is also an
/// `ObservableAny`.
impl<T, E, S, O> ObservableAny<T, E> for O
where
    T: Send,
    E: Send,
    S: Any + Send,
    O: Observable<T, E, Subscription = S>,
{
    fn subscribe_any(
        &mut self,
        observer: ObserverBox<T, E>,
    ) -> Result<Box<dyn Any + Send>, ObserverBox<T, E>> {
        self.subscribe(observer)
            .map(|s| Box::new(s) as Box<dyn Any + Send>)
    }

    fn unsubscribe_any(&mut self, subscription: &dyn Any) -> Option<ObserverBox<T, E>> {
        subscription
            .downcast_ref::<S>()
            .and_then(|s| self.unsubscribe(s))
    }
}

/// A very simple observable that supports subscription of a single
/// observer.
#[derive(Debug, Default)]
pub struct UpdatesObservable<T, E> {
    /// A reference to the `Observer` subscribed to us, if any.
    pub observer: SharedObserver<OptionalObserver<ObserverBox<T, E>>>,
}

impl<T, E> Observable<T, E> for UpdatesObservable<T, E>
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

    use crate::observe::test::MockObserver;

    /// Test subscribing and unsubscribing for an `UpdatesObservable`.
    #[test]
    fn subscribe_unsubscribe() {
        let mut observable = UpdatesObservable::<(), ()>::default();
        let observer = Box::new(MockObserver::new());

        assert!(observable.subscribe(observer).is_ok());
        assert!(observable.unsubscribe(&()).is_some());
    }

    /// Test multiple subscriptions to an `UpdatesObservable`.
    #[test]
    fn multiple_subscribe() {
        let mut observable = UpdatesObservable::<(), ()>::default();
        let observer1 = Box::new(MockObserver::new());
        let observer2 = Box::new(MockObserver::new());

        assert!(observable.subscribe(observer1).is_ok());
        assert!(observable.subscribe(observer2).is_err());
    }

    /// Test subscribing and unsubscribing through an `ObservableAny`.
    #[test]
    fn subscribe_unsubscribe_any() {
        let mut observable = UpdatesObservable::<(), ()>::default();
        let observer = Box::new(MockObserver::new());

        let subscription = observable.subscribe_any(observer).unwrap();
        assert!(observable.unsubscribe_any(subscription.as_ref()).is_some());
    }
}
