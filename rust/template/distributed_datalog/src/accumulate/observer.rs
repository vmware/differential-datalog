use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::FromIterator;

use log::trace;
use uid::Id;

use differential_datalog::program::RelId;
use differential_datalog::program::Update;

use crate::Observable;
use crate::ObservableBox;
use crate::Observer;
use crate::ObserverBox;
use crate::OptionalObserver;
use crate::SharedObserver;

/// Wrapper around a `SharedObserver` that inspects the updates to derive the current state.
/// Apart from that it simply forwards all messages to the observer.
#[derive(Debug)]
pub struct AccumulatingObserver<T, V, E>
where
    V: Debug + Eq + Hash,
{
    id: usize,
    /// The observable we track and our subscription to it. TODO: use a better data structure
    subscription: Option<(ObservableBox<T, E>, Box<dyn Any + Send>)>,
    /// The observer we ultimately push our data to.
    observer: SharedObserver<OptionalObserver<ObserverBox<T, E>>>,
    /// The data we accumulated so far.
    data: HashMap<RelId, HashSet<V>>,
    /// Temporary buffer to cache the updates before committing.
    buffer: Option<LinkedList<Vec<T>>>,
}

impl<T, V, E> AccumulatingObserver<T, V, E>
where
    V: Clone + Debug + Eq + Hash + Send + 'static,
{
    pub fn new() -> Self {
        let id = Id::<()>::new().get();
        trace!("AccumulatingObserver({})::new", id);

        Self {
            id,
            subscription: None,
            observer: SharedObserver::default(),
            data: HashMap::new(),
            buffer: None,
        }
    }

    pub fn get_current_state(&self) -> HashMap<RelId, HashSet<V>> {
        trace!("AccumulatingObserver({})::get_current_state()", self.id);
        self.data.clone()
    }

    pub fn clear_and_return_state(&mut self) -> HashMap<RelId, HashSet<V>> {
        trace!(
            "AccumulatingObserver({})::clear_and_return_state()",
            self.id
        );
        let _ = self.buffer.take();
        self.data.drain().collect()
    }
}

impl<T, V, E> Observable<T, E> for AccumulatingObserver<T, V, E>
where
    T: Debug + Send + 'static,
    V: Debug + Eq + Hash,
    E: Debug + Send + 'static,
{
    type Subscription = ();

    fn subscribe(
        &mut self,
        observer: ObserverBox<T, E>,
    ) -> Result<Self::Subscription, ObserverBox<T, E>> {
        trace!("AccumulatingObserver({})::subscribe()", self.id);
        let mut guard = self.observer.lock().unwrap();
        if guard.is_some() {
            Err(observer)
        } else {
            let _ = guard.replace(observer);
            Ok(())
        }
    }

    fn unsubscribe(&mut self, _subscription: &Self::Subscription) -> Option<ObserverBox<T, E>> {
        trace!("AccumulatingObserver({})::unsubscribe()", self.id);
        self.observer.lock().unwrap().take()
    }
}

/// Forwards the incoming data to the observer while keeping track of the current state
impl<V, E> Observer<Update<V>, E> for AccumulatingObserver<Update<V>, V, E>
where
    V: Debug + Send + Eq + Hash + Clone,
    E: Debug + Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        trace!("AccumulatingObserver({})::on_start", self.id);

        if self.buffer.is_some() {
            panic!("received multiple on_start events")
        } else {
            self.buffer = Some(LinkedList::new());
            let mut guard = self.observer.lock().unwrap();
            guard.on_start()
        }
    }

    fn on_commit(&mut self) -> Result<(), E> {
        trace!("AccumulatingObserver({})::on_commit", self.id);

        if let Some(buffer) = self.buffer.take() {
            // forward commit signal to observer
            {
                let mut guard = self.observer.lock().unwrap();
                guard.on_commit()?;
            }
            // apply the buffered updates to the accumulated state if successful
            buffer
                .into_iter()
                .flatten()
                .for_each(|upd: Update<V>| match upd {
                    Update::Insert { relid, v } => {
                        let _ = self
                            .data
                            .entry(relid)
                            .and_modify(|set| {
                                let _ = set.insert(v.clone());
                            })
                            .or_insert_with(|| HashSet::from_iter(vec![v.clone()].into_iter()));
                    }
                    Update::DeleteValue { relid, v } => {
                        let _ = self.data.entry(relid).and_modify(|set| {
                            let _ = set.remove(&v);
                        });
                    }
                    update => panic!("Operation {:?} not allowed", update),
                });

            Ok(())
        } else {
            panic!("on_commit was not preceded by an on_start event")
        }
    }

    fn on_updates<'a>(
        &mut self,
        updates: Box<dyn Iterator<Item = Update<V>> + 'a>,
    ) -> Result<(), E> {
        trace!("AccumulatingObserver({})::on_updates", self.id);

        if let Some(ref mut buffer) = self.buffer {
            // push incoming updates into buffer
            let upds = updates.collect::<Vec<_>>();
            buffer.push_back(upds.clone());

            // send updates to observer
            let mut guard = self.observer.lock().unwrap();
            guard.on_updates(Box::new(upds.into_iter()))
        } else {
            panic!("on_updates was not preceded by an on_start event")
        }
    }

    /// signals that the source has been removed, clears the accumulated state.
    fn on_completed(&mut self) -> Result<(), E> {
        trace!("AccumulatingObserver({})::on_completed", self.id);
        let _ = self.buffer.take();
        let _ = self.data.drain();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::vec::IntoIter;

    use crate::MockObserver;

    fn get_usize_insert_updates_1() -> Box<IntoIter<Update<usize>>> {
        Box::new(
            vec![
                Update::Insert { relid: 1, v: 1 },
                Update::Insert { relid: 2, v: 2 },
                Update::Insert { relid: 3, v: 3 },
            ]
            .into_iter(),
        )
    }

    fn get_usize_insert_updates_2() -> Box<IntoIter<Update<usize>>> {
        Box::new(
            vec![
                Update::Insert { relid: 1, v: 2 },
                Update::Insert { relid: 1, v: 3 },
                Update::Insert { relid: 2, v: 3 },
            ]
            .into_iter(),
        )
    }

    fn get_usize_insert_updates_3() -> Box<IntoIter<Update<usize>>> {
        Box::new(
            vec![
                Update::Insert { relid: 4, v: 1 },
                Update::Insert { relid: 4, v: 2 },
                Update::Insert { relid: 4, v: 3 },
                Update::Insert { relid: 4, v: 4 },
            ]
            .into_iter(),
        )
    }

    fn get_usize_delete_updates_1() -> Box<IntoIter<Update<usize>>> {
        Box::new(
            vec![
                Update::DeleteValue { relid: 1, v: 1 },
                Update::DeleteValue { relid: 2, v: 2 },
                Update::DeleteValue { relid: 3, v: 3 },
            ]
            .into_iter(),
        )
    }

    /// Test subscribing and unsubscribing for an `AccumulatingObserver`.
    #[test]
    fn subscribe_unsubscribe_observable() {
        let mut observer = AccumulatingObserver::<Update<()>, (), ()>::new();
        let mock = Box::new(MockObserver::new());

        let subscription = observer.subscribe(mock);
        assert!(subscription.is_ok());
        assert!(observer.unsubscribe(&subscription.unwrap()).is_some());
        assert!(observer.observer.lock().unwrap().is_none());
    }

    /// Test multiple subscriptions to an `AccumulatingObserver`.
    #[test]
    fn multiple_subscribe_observable() {
        let mut observer = AccumulatingObserver::<Update<usize>, usize, ()>::new();
        let mock1 = Box::new(MockObserver::new());
        let mock2 = Box::new(MockObserver::new());

        assert!(observer.subscribe(mock1).is_ok());
        assert!(observer.subscribe(mock2).is_err());
    }

    /// Test pass-through filter behaviour for transactions via a `AccumulatingObserver`.
    #[test]
    fn transparent_transactions_proxy() {
        let mut observer = AccumulatingObserver::<Update<usize>, usize, ()>::new();
        let mock = Arc::new(Mutex::new(Some(MockObserver::default())));
        let _subscription = observer.subscribe(Box::new(mock.clone()));

        assert_eq!(observer.on_start(), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_start, 1);

        assert_eq!(observer.on_updates(get_usize_insert_updates_1()), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_updates, 3);

        assert_eq!(observer.on_updates(get_usize_insert_updates_2()), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_updates, 6);

        assert_eq!(observer.on_commit(), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_commit, 1);

        assert_eq!(observer.on_start(), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_start, 2);

        assert_eq!(observer.on_updates(get_usize_insert_updates_3()), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_updates, 10);

        assert_eq!(observer.on_commit(), Ok(()));
        assert_eq!(mock.lock().unwrap().as_ref().unwrap().called_on_commit, 2);

        assert_eq!(observer.on_completed(), Ok(()));
        assert_eq!(
            mock.lock().unwrap().as_ref().unwrap().called_on_completed,
            0
        );
    }

    /// test accumulation of data across multiple commits
    #[test]
    fn updates_accumulation() {
        let mut observer = AccumulatingObserver::<Update<usize>, usize, ()>::new();
        let mock = Arc::new(Mutex::new(Some(MockObserver::default())));
        let _subscription = observer.subscribe(Box::new(mock.clone()));

        assert_eq!(observer.on_start(), Ok(()));
        assert_eq!(observer.on_updates(get_usize_insert_updates_1()), Ok(()));
        assert_eq!(observer.on_updates(get_usize_insert_updates_2()), Ok(()));

        assert!(observer.data.is_empty());
        assert_eq!(observer.on_commit(), Ok(()));

        observer
            .data
            .iter()
            .for_each(|(relid, values)| match relid {
                &1 => {
                    assert_eq!(values, &vec!(1, 2, 3).into_iter().collect::<HashSet<_>>());
                }
                &2 => {
                    assert_eq!(values, &vec!(2, 3).into_iter().collect::<HashSet<_>>());
                }
                &3 => {
                    assert_eq!(values, &vec!(3).into_iter().collect::<HashSet<_>>());
                }
                _ => panic!("Unexpected relid!"),
            });

        assert_eq!(observer.on_start(), Ok(()));
        assert_eq!(observer.on_updates(get_usize_delete_updates_1()), Ok(()));
        assert_eq!(observer.on_updates(get_usize_insert_updates_3()), Ok(()));

        // data must not be updated before commit
        observer
            .data
            .iter()
            .for_each(|(relid, values)| match relid {
                &1 => {
                    assert_eq!(values, &vec!(1, 2, 3).into_iter().collect::<HashSet<_>>());
                }
                &2 => {
                    assert_eq!(values, &vec!(2, 3).into_iter().collect::<HashSet<_>>());
                }
                &3 => {
                    assert_eq!(values, &vec!(3).into_iter().collect::<HashSet<_>>());
                }
                _ => panic!("Unexpected relid!"),
            });

        assert_eq!(observer.on_commit(), Ok(()));

        observer
            .data
            .iter()
            .for_each(|(relid, values)| match relid {
                &1 => {
                    assert_eq!(values, &vec!(2, 3).into_iter().collect::<HashSet<_>>());
                }
                &2 => {
                    assert_eq!(values, &vec!(3).into_iter().collect::<HashSet<_>>());
                }
                &3 => {
                    assert!(values.is_empty());
                }
                &4 => {
                    assert_eq!(
                        values,
                        &vec!(1, 2, 3, 4).into_iter().collect::<HashSet<_>>()
                    );
                }
                _ => panic!("Unexpected relid!"),
            });
    }
}
