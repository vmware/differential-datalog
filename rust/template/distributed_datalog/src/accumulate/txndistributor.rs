use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

use log::trace;
use uid::Id;

use crate::{Observable, UpdatesObservable};
use crate::Observer;
use crate::ObserverBox;
use crate::OptionalObserver;
use crate::SharedObserver;


#[derive(Debug)]
pub struct TxnDistributor<T, E> {
    id: usize,
    /// A list of references to the `Observers` subscribed to us, if any.
    /// The map is indexed by an ID indicating the subscription of each observer.
    observers: HashMap<usize, SharedObserver<OptionalObserver<ObserverBox<T, E>>>>,
}


impl<T, E> TxnDistributor<T, E>
    where
        T: Debug + Send + 'static,
        E: Debug + Send + 'static,
{
    pub fn new() -> Self {
        let id = Id::<()>::new().get();
        trace!("TxnDistributor({})::new", id);

        Self {
            id,
            observers: HashMap::new(),
        }
    }

    pub fn create_observable(
        &mut self,
    ) -> UpdatesObservable<T, E> {
        let subscription = Id::<()>::new().get();
        trace!("TxnDistributor({:?})::create_observable({:?})", self.id, subscription);

        let observer = SharedObserver::default();
        let _ = self.observers.insert(subscription, observer.clone());
        UpdatesObservable {
            observer
        }
    }
}


impl<T, E> Observable<T, E> for TxnDistributor<T, E>
    where
        T: Debug + Send + 'static,
        E: Debug + Send + 'static,
{
    type Subscription = usize;

    fn subscribe(
        &mut self,
        observer: ObserverBox<T, E>,
    ) -> Result<Self::Subscription, ObserverBox<T, E>> {
        let id = Id::<()>::new().get();
        trace!("TxnDistributor({})::subscribe({})", self.id, id);

        // TODO: can the same observer subscribe multiple times?
        let _ = self.observers.insert(id, Arc::new(Mutex::new(Some(observer))));
        Ok(id)
    }

    fn unsubscribe(&mut self, subscription: &Self::Subscription) -> Option<ObserverBox<T, E>> {
        trace!("TxnDistributor({})::unsubscribe({})", self.id, subscription);
        match self.observers.remove(subscription) {
            Some(observer) => Some(Box::new(observer)),
            None => None
        }
    }
}


/// Receives the values, clones them and sends them to each observer
impl<T, E> Observer<T, E> for TxnDistributor<T, E>
    where
        T: Send + Debug + Clone,
        E: Send + Debug
{
    fn on_start(&mut self) -> Result<(), E> {
        trace!("TxnDistributor({})::on_start", self.id);

        match self.observers.values_mut()
            .map(|o| o.on_start())
            .collect::<Result<Vec<_>, E>>() // collects all results into a single result
        {
            Ok(_) => Ok(()),
            Err(error) => Err(error)
        }
    }

    fn on_commit(&mut self) -> Result<(), E> {
        trace!("TxnDistributor({})::on_commit", self.id);

        match self.observers.values_mut()
            .map(|o| o.on_commit())
            .collect::<Result<Vec<_>, E>>() // collects all results into a single result
        {
            Ok(_) => Ok(()),
            Err(error) => Err(error)
        }
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item=T> + 'a>) -> Result<(), E> {
        trace!("TxnDistributor({})::on_updates", self.id);

        // clone updates for each observer
        let upd_vec = updates.collect::<Vec<T>>();
        match self.observers.values_mut()
            .map(move |o|
                o.on_updates(Box::new(upd_vec.clone().into_iter()))
            )
            .collect::<Result<Vec<_>, E>>() // collects all results into a single result
        {
            Ok(_) => Ok(()),
            Err(error) => Err(error)
        }
    }


    fn on_completed(&mut self) -> Result<(), E> {
        trace!("TxnDistributor({})::on_completed", self.id);
        match self.observers.values_mut()
            .map(|o| o.on_completed())
            .collect::<Result<Vec<_>, E>>() // collects all results into a single result
        {
            Ok(_) => Ok(()),
            Err(error) => Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::MockObserver;

    /// Test subscribing and unsubscribing for a `TxnDistributor`.
    /// A subscription can occur directly via `subscribe` or via `create_observable`.
    #[test]
    fn subscribe_unsubscribe_distributor() {
        let mut distributor = TxnDistributor::<(), ()>::new();
        let observer = Box::new(MockObserver::new());

        let subscription = distributor.subscribe(observer);
        assert!(subscription.is_ok());
        assert!(distributor.unsubscribe(&subscription.unwrap()).is_some());
        assert!(distributor.observers.values().collect::<Vec<_>>().is_empty());

        let mut observable = distributor.create_observable();
        let observer = Box::new(MockObserver::new());

        let subscription = observable.subscribe(observer);
        assert!(subscription.is_ok());
        assert!(observable.unsubscribe(&subscription.unwrap()).is_some());
        assert!(observable.observer.lock().unwrap().is_none());
    }

    /// Test multiple direct subscriptions via `subscribe` to a `TxnDistributor`.
    #[test]
    fn multiple_subscribe_direct_distributor() {
        let mut distributor = TxnDistributor::<_, ()>::new();
        let mock1 = Arc::new(Mutex::new(MockObserver::new()));
        let mock2 = Arc::new(Mutex::new(MockObserver::new()));

        assert!(distributor.subscribe(Box::new(mock1.clone())).is_ok());
        assert!(distributor.subscribe(Box::new(mock2.clone())).is_ok());

        assert_eq!(distributor.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 1);
        assert_eq!(mock2.lock().unwrap().called_on_start, 1);

        assert_eq!(distributor.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 3);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 3);

        assert_eq!(distributor.on_commit(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_commit, 1);
        assert_eq!(mock2.lock().unwrap().called_on_commit, 1);

        assert_eq!(distributor.on_completed(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_completed, 1);
        assert_eq!(mock2.lock().unwrap().called_on_completed, 1);
    }

    /// Test multiple indirect subscriptions via `create_observable` to a `TxnDistributor`.
    #[test]
    fn multiple_subscribe_indirect_distributor() {
        let mut distributor = TxnDistributor::<_, ()>::new();
        let mock1 = Arc::new(Mutex::new(MockObserver::new()));
        let mock2 = Arc::new(Mutex::new(MockObserver::new()));
        let mut observable1 = distributor.create_observable();
        let mut observable2 = distributor.create_observable();

        assert!(observable1.subscribe(Box::new(mock1.clone())).is_ok());
        assert!(observable2.subscribe(Box::new(mock2.clone())).is_ok());

        assert_eq!(distributor.on_start(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_start, 1);
        assert_eq!(mock2.lock().unwrap().called_on_start, 1);

        assert_eq!(distributor.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_updates, 3);
        assert_eq!(mock2.lock().unwrap().called_on_updates, 3);

        assert_eq!(distributor.on_commit(), Ok(()));
        assert_eq!(mock1.lock().unwrap().called_on_commit, 1);
        assert_eq!(mock2.lock().unwrap().called_on_commit, 1);
    }
}




