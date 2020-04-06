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

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;

use log::trace;
use uid::Id;

use differential_datalog::program::Update;
use differential_datalog::program::RelId;

use crate::Observable;
use crate::Observer;
use crate::ObserverBox;

use crate::accumulator::AccumulatingObserver;
use crate::accumulator::TxnDistributor;
use crate::accumulator::txndistributor::InitializedObservable;

/// A trait object that acts as a proxy between an observable and observer.
/// It accumulates the updates to maintain the current state of the data.
pub trait Accumulator<V, E>: Observer<Update<V>, E> + Observable<Update<V>, E>
    where
        V: Send + Debug + Eq + Hash,
        E: Send,
{
    /// Creates a new Accumulator without any subscriptions or subscribers.
    fn new() -> Self;

    /// Returns a new Observable that can be used to listen to the outputs of the Accumulator.
    fn create_observable(&mut self) -> InitializedObservable<Update<V>, E>;

    /// Return the current state of the data.
    fn get_current_state(&self) -> HashMap<RelId, HashSet<V>>;
}

/// An Accumulator implementation that can have multiple observers (can be subscribed to more
/// than once). Spawns an `AccumulatingObserver` to which a `TxnDistributor` is subscribed to.
#[derive(Debug)]
pub struct DistributingAccumulator<T, V, E>
    where
        T: Debug + Send,
        V: Debug + Eq + Hash + Send,
        E: Debug + Send,
{
    /// The accumulator's unique ID.
    id: usize,
    /// Component responsible for accumulating the data.
    observer: AccumulatingObserver<T, V, E>,
    /// Component responsible for distributing the output to multiple observers.
    distributor: Arc<Mutex<TxnDistributor<T, E>>>,
}

impl<V, E> Accumulator<V, E> for DistributingAccumulator<Update<V>, V, E>
    where
        V: Debug + Send + Clone + Eq + Hash + 'static,
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
            distributor,
        }
    }

    fn create_observable(&mut self) -> InitializedObservable<Update<V>, E> {
        trace!("DistributingAccumulator({})::create_observable()", self.id);
        let init_updates =
            self.get_current_state().into_iter()
                .flat_map(|(relid, vs)|
                    vs.into_iter().map(|v| Update::Insert { relid, v }).collect::<Vec<_>>())
                .collect::<Vec<_>>();

        let mut guard = self.distributor.lock().unwrap();
        guard.create_observable(init_updates)
    }


    fn get_current_state(&self) -> HashMap<RelId, HashSet<V>> {
        trace!("DistributingAccumulator({})::get_current_state()", self.id);
        self.observer.get_current_state()
    }
}

/// The methods for the Observable trait are delegated to the TxnDistributor
impl<T, V, E> Observable<T, E> for DistributingAccumulator<T, V, E>
    where
        T: Debug + Send + 'static,
        V: Debug + Send + Eq + Hash,
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
impl<V, E> Observer<Update<V>, E> for DistributingAccumulator<Update<V>, V, E>
    where
        V: Debug + Send + Eq + Hash + Clone,
        E: Debug + Send
{
    fn on_start(&mut self) -> Result<(), E> {
        trace!("DistributingAccumulator({})::on_start", self.id);
        self.observer.on_start()
    }

    fn on_commit(&mut self) -> Result<(), E> {
        trace!("DistributingAccumulator({})::on_commit", self.id);
        self.observer.on_commit()
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item=Update<V>> + 'a>) -> Result<(), E> {
        trace!("DistributingAccumulator({})::on_updates", self.id);
        self.observer.on_updates(updates)
    }

    fn on_completed(&mut self) -> Result<(), E> {
        trace!("DistributingAccumulator({})::on_completed", self.id);
        self.observer.on_completed()
    }
}


#[cfg(test)]
pub mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::vec::IntoIter;

    use crate::MockObserver;

    fn get_usize_updates_1() -> Box<IntoIter<Update<usize>>> {
        Box::new(vec!(
            Update::Insert { relid: 1, v: 1 },
            Update::Insert { relid: 2, v: 2 },
            Update::Insert { relid: 3, v: 3 },
        ).into_iter())
    }

    fn get_usize_updates_2() -> Box<IntoIter<Update<usize>>> {
        Box::new(vec!(
            Update::Insert { relid: 1, v: 2 },
            Update::Insert { relid: 1, v: 3 },
            Update::Insert { relid: 2, v: 3 },
        ).into_iter())
    }

    fn get_usize_updates_3() -> Box<IntoIter<Update<usize>>> {
        Box::new(vec!(
            Update::Insert { relid: 4, v: 1 },
            Update::Insert { relid: 4, v: 2 },
            Update::Insert { relid: 4, v: 3 },
            Update::Insert { relid: 4, v: 4 },
        ).into_iter())
    }

    //TODO: test accumulation of data across multiple commits

    /// Test subscribing and unsubscribing for a `DistributingAccumulator`.
    /// A subscription can occur directly via `subscribe` or via `create_observable`.
    #[test]
    fn subscribe_unsubscribe() {
        let mut accumulator = DistributingAccumulator::<Update<()>, (), ()>::new();
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
        let mut accumulator = DistributingAccumulator::<Update<usize>, usize, ()>::new();
        let mock1 = Arc::new(Mutex::new(MockObserver::new()));
        let mock2 = Arc::new(Mutex::new(MockObserver::new()));

        assert!(accumulator.subscribe(Box::new(mock1.clone())).is_ok());
        assert!(accumulator.subscribe(Box::new(mock2.clone())).is_ok());

        assert_eq!(accumulator.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 1);
        assert_eq!(mock2.lock().unwrap().called_on_start, 1);

        assert_eq!(accumulator.on_updates(get_usize_updates_1()), Ok(()));
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
        let mut accumulator = DistributingAccumulator::<Update<usize>, usize, ()>::new();
        let mock1 = Arc::new(Mutex::new(MockObserver::new()));
        let mock2 = Arc::new(Mutex::new(MockObserver::new()));
        let mut observable1 = accumulator.create_observable();
        let mut observable2 = accumulator.create_observable();

        assert!(observable1.subscribe(Box::new(mock1.clone())).is_ok());
        assert!(observable2.subscribe(Box::new(mock2.clone())).is_ok());

        assert_eq!(accumulator.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 1);
        assert_eq!(mock2.lock().unwrap().called_on_start, 1);

        assert_eq!(accumulator.on_updates(get_usize_updates_1()), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 3);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 3);

        assert_eq!(accumulator.on_commit(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_commit, 1);
        assert_eq!(mock2.lock().unwrap().called_on_commit, 1);
    }

    /// Test pass-through filter behaviour for transactions via a `DistributingAccumulator`.
    #[test]
    fn transparent_transactions_proxy() {
        let mut accumulator = DistributingAccumulator::<Update<usize>, usize, ()>::new();
        let mock1 = Arc::new(Mutex::new(MockObserver::new()));
        let mock2 = Arc::new(Mutex::new(MockObserver::new()));
        let mut observable = accumulator.create_observable();

        assert!(observable.subscribe(Box::new(mock1.clone())).is_ok());
        assert!(accumulator.subscribe(Box::new(mock2.clone())).is_ok());

        assert_eq!(accumulator.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 1);
        assert_eq!(mock2.lock().unwrap().called_on_start, 1);

        assert_eq!(accumulator.on_updates(get_usize_updates_1()), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 3);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 3);

        assert_eq!(accumulator.on_updates(get_usize_updates_2()), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 6);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 6);

        assert_eq!(accumulator.on_commit(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_commit, 1);
        assert_eq!(mock2.lock().unwrap().called_on_commit, 1);

        assert_eq!(accumulator.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 2);
        assert_eq!(mock2.lock().unwrap().called_on_start, 2);

        assert_eq!(accumulator.on_updates(get_usize_updates_3()), Ok(()));
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

