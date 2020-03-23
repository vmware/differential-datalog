//! D3Log Accumulator
//! Defines the Accumulator trait, which is an Observer and Observable simultaneously. Implements
//! the trait with a DistributingAccumulator that observes a single observable and can be
//! subscribed by multiple observers.
//!
//! Conceptually, the DistributingAccumulator consists of two different components:
//! the AccumulatingObserver and a TxnDistributor. It encapsulates the functionality
//! of those components in a single structure.
//! The AccumulatingObserver is a simple proxy between an observable and an observer that
//! inspects the data that passes through. It accumulates the data to keep track of the
//! current state.
//! The TxnDistributor is the inverse of the `TxnMux` class, it listens to a single observable and
//! is able to send data to multiple observers.

use std::collections::LinkedList;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

use log::trace;
use uid::Id;

use crate::Observable;
use crate::Observer;
use crate::ObserverBox;
use crate::ObservableBox;

use crate::accumulator::AccumulatingObserver;
use crate::accumulator::TxnDistributor;
use crate::accumulator::txndistributor::InitializedObservable;

/// A trait object that acts as a proxy between an observable and observer.
/// It accumulates the updates to maintain the current state of the data.
pub trait Accumulator<T, E>: Observer<T, E> + Observable<T, E>
    where
        T: Send,
        E: Send,
{
    /// Creates a new Accumulator without any subscriptions or subscribers.
    fn new() -> Self;

    /// Returns a new Observable that can be used to listen to the outputs of the Accumulator.
    fn create_observable(&mut self) -> InitializedObservable<T, E>;

    /// Return the current state of the data.
    fn get_current_state(&self) -> Option<LinkedList<Vec<T>>>;
}

/// An Accumulator implementation that can have multiple observers (can be subscribed to more
/// than once). Spawns an `AccumulatingObserver` to which a `TxnDistributor` is subscribed to.
#[derive(Debug)]
pub struct DistributingAccumulator<T, E>
    where
        T: Debug + Send,
        E: Debug + Send,
{
    /// The accumulator's unique ID.
    id: usize,
    /// Component responsible for accumulating the data.
    observer: AccumulatingObserver<T, E>,
    /// Component responsible for distributing the output to multiple observers.
    distributor: Arc<Mutex<TxnDistributor<T, E>>>,
}

impl<T, E> Accumulator<T, E> for DistributingAccumulator<T, E>
    where
        T: Debug + Send + Clone + 'static,
        E: Debug + Send + 'static,
{
    fn new() -> Self {
        let id = Id::<()>::new().get();
        trace!("DistributingAccumulator({})::new", id);

        // Instantiate AccumulatingObserver and TxnDistributor and subscribe the latter to the former
        let mut observer = AccumulatingObserver::new();
        let distributor = Arc::new(Mutex::new(TxnDistributor::new()));
        let _subscription = observer.subscribe(Box::new(distributor.clone()));

        Self {
            id,
            observer,
            distributor
        }
    }

    fn create_observable(&mut self) -> InitializedObservable<T, E> {
        trace!("DistributingAccumulator({})::create_observable()", self.id);
        let init_data = self.get_current_state();
        let mut guard = self.distributor.lock().unwrap();
        guard.create_observable(init_data)
    }

    fn get_current_state(&self) -> Option<LinkedList<Vec<T>>> {
        trace!("DistributingAccumulator({})::get_current_state()", self.id);
        self.observer.get_current_state()
    }
}

/// The methods for the Observable trait are delegated to the TxnDistributor
impl<T, E> Observable<T, E> for DistributingAccumulator<T, E>
    where
        T: Debug + Send + 'static,
        E: Debug + Send + 'static,
{
    type Subscription = usize;

    fn subscribe(
        &mut self,
        observer: ObserverBox<T, E>,
    ) -> Result<Self::Subscription, ObserverBox<T, E>> {
        trace!("DistributingAccumulator({})::subscribe()", self.id);
        self.distributor.subscribe(observer)
    }

    fn unsubscribe(&mut self, subscription: &Self::Subscription) -> Option<ObserverBox<T, E>> {
        trace!("DistributingAccumulator({})::unsubscribe({})", self.id, subscription);
        self.distributor.unsubscribe(subscription)
    }
}

/// The methods for the Observer trait are delegated to the AccumulatingObserver
impl<T, E> Observer<T, E> for DistributingAccumulator<T, E>
    where
        T: Send + Debug + Clone,
        E: Send + Debug
{
    fn on_start(&mut self) -> Result<(), E> {
        trace!("DistributingAccumulator({})::on_start", self.id);
        self.observer.on_start()
    }

    fn on_commit(&mut self) -> Result<(), E> {
        trace!("DistributingAccumulator({})::on_commit", self.id);
        self.observer.on_commit()
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item=T> + 'a>) -> Result<(), E> {
        trace!("DistributingAccumulator({})::on_updates", self.id);
        self.observer.on_updates(updates)
    }

    fn on_completed(&mut self) -> Result<(), E> {
        trace!("DistributingAccumulator({})::on_completed", self.id);
        self.observer.on_completed()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;

    use crate::MockObserver;
    use crate::observe::ObservableAny;

    //TODO: test accumulation of data across multiple commits

    /// Test subscribing and unsubscribing for a `DistributingAccumulator`.
    /// A subscription can occur directly via `subscribe` or via `create_observable`.
    #[test]
    fn subscribe_unsubscribe() {
        let mut accumulator = DistributingAccumulator::<(), ()>::new();
        let observer = Box::new(MockObserver::new());

        let subscription = accumulator.subscribe(observer);
        assert!(subscription.is_ok());
        assert!(accumulator.unsubscribe(&subscription.unwrap()).is_some());

        let mut observable = accumulator.create_observable();
        let observer = Box::new(MockObserver::new());

        let subscription = observable.subscribe(observer);
        assert!(subscription.is_ok());
        assert!(observable.unsubscribe(&subscription.unwrap()).is_some());
    }

    /// Test multiple direct subscriptions via `subscribe` to a `AccumulatingObserver`.
    #[test]
    fn multiple_subscribe_direct_distributor() {
        let mut accumulator = DistributingAccumulator::<_, ()>::new();
        let mock1 = Arc::new(Mutex::new(MockObserver::new()));
        let mock2 = Arc::new(Mutex::new(MockObserver::new()));

        assert!(accumulator.subscribe(Box::new(mock1.clone())).is_ok());
        assert!(accumulator.subscribe(Box::new(mock2.clone())).is_ok());

        assert_eq!(accumulator.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 1);
        assert_eq!(mock2.lock().unwrap().called_on_start, 1);

        assert_eq!(accumulator.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 3);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 3);

        assert_eq!(accumulator.on_commit(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_commit, 1);
        assert_eq!(mock2.lock().unwrap().called_on_commit, 1);

        assert_eq!(accumulator.on_completed(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_completed, 1);
        assert_eq!(mock2.lock().unwrap().called_on_completed, 1);
    }

    /// Test multiple indirect subscriptions via `create_observable` to a `DistributingAccumulator`.
    #[test]
    fn multiple_subscribe_indirect_distributor() {
        let mut accumulator = DistributingAccumulator::<_, ()>::new();
        let mock1 = Arc::new(Mutex::new(MockObserver::new()));
        let mock2 = Arc::new(Mutex::new(MockObserver::new()));
        let mut observable1 = accumulator.create_observable();
        let mut observable2 = accumulator.create_observable();

        assert!(observable1.subscribe(Box::new(mock1.clone())).is_ok());
        assert!(observable2.subscribe(Box::new(mock2.clone())).is_ok());

        assert_eq!(accumulator.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 1);
        assert_eq!(mock2.lock().unwrap().called_on_start, 1);

        assert_eq!(accumulator.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 3);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 3);

        assert_eq!(accumulator.on_commit(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_commit, 1);
        assert_eq!(mock2.lock().unwrap().called_on_commit, 1);
    }

    /// Test subscribing and unsubscribing through an `ObservableAny`.
    #[test]
    fn subscribe_unsubscribe_any_observable() {
        let mut accumulator = DistributingAccumulator::<(), ()>::new();
        let mock = Box::new(MockObserver::new());

        let subscription = accumulator.subscribe_any(mock).unwrap();
        assert!(accumulator.unsubscribe_any(subscription.as_ref()).is_some());
    }

    /// Test pass-through filter behaviour for transactions via a `DistributingAccumulator`.
    #[test]
    fn transparent_transactions_proxy() {
        let mut accumulator = DistributingAccumulator::<_, ()>::new();
        let mock1 = Arc::new(Mutex::new(MockObserver::new()));
        let mock2 = Arc::new(Mutex::new(MockObserver::new()));
        let mut observable = accumulator.create_observable();

        assert!(observable.subscribe(Box::new(mock1.clone())).is_ok());
        assert!(accumulator.subscribe(Box::new(mock2.clone())).is_ok());

        assert_eq!(accumulator.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 1);
        assert_eq!(mock2.lock().unwrap().called_on_start, 1);

        assert_eq!(accumulator.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 3);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 3);

        assert_eq!(accumulator.on_updates(Box::new([6, 4, 5].iter())), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 6);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 6);

        assert_eq!(accumulator.on_commit(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_commit, 1);
        assert_eq!(mock2.lock().unwrap().called_on_commit, 1);

        assert_eq!(accumulator.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 2);
        assert_eq!(mock2.lock().unwrap().called_on_start, 2);

        assert_eq!(accumulator.on_updates(Box::new([7, 9, 8, 10].iter())), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 10);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 10);

        assert_eq!(accumulator.on_commit(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_commit, 2);
        assert_eq!(mock2.lock().unwrap().called_on_commit, 2);

        assert_eq!(accumulator.on_completed(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_completed, 1);
        assert_eq!(mock2.lock().unwrap().called_on_completed, 1);
    }
}

