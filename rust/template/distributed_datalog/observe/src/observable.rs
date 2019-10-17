use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

use crate::observer::ObserverBox;

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

/// A very simple observable that supports subscription of a single
/// observer.
#[derive(Debug)]
pub struct UpdatesObservable<T, E> {
    /// A reference to the `Observer` we subscribed to.
    pub observer: Arc<Mutex<Option<ObserverBox<T, E>>>>,
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
        match *guard {
            Some(_) => Err(observer),
            None => {
                *guard = Some(observer);
                Ok(())
            }
        }
    }

    fn unsubscribe(&mut self, _subscription: &Self::Subscription) -> Option<ObserverBox<T, E>> {
        let mut guard = self.observer.lock().unwrap();
        match *guard {
            Some(_) => guard.take(),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::Observer;

    #[derive(Debug)]
    struct DummyObserver(());

    impl Observer<(), ()> for DummyObserver {
        fn on_start(&mut self) -> Result<(), ()> {
            Ok(())
        }

        fn on_commit(&mut self) -> Result<(), ()> {
            Ok(())
        }

        fn on_updates<'a>(&mut self, _: Box<dyn Iterator<Item = ()> + 'a>) -> Result<(), ()> {
            Ok(())
        }

        fn on_completed(&mut self) -> Result<(), ()> {
            Ok(())
        }
    }

    /// Test subscribing and unsubscribing for an `UpdatesObservable`.
    #[test]
    fn subscribe_unsubscribe() {
        let mut observable = UpdatesObservable {
            observer: Arc::new(Mutex::new(None)),
        };
        let observer = Box::new(DummyObserver(()));

        assert!(observable.subscribe(observer).is_ok());
        assert!(observable.unsubscribe(&()).is_some());
    }

    /// Test multiple subscriptions to an `UpdatesObservable`.
    #[test]
    fn multiple_subscribe() {
        let mut observable = UpdatesObservable {
            observer: Arc::new(Mutex::new(None)),
        };
        let observer1 = Box::new(DummyObserver(()));
        let observer2 = Box::new(DummyObserver(()));

        assert!(observable.subscribe(observer1).is_ok());
        assert!(observable.subscribe(observer2).is_err());
    }
}
