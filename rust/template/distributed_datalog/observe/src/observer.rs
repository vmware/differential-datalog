use std::collections::LinkedList;
use std::fmt::Debug;
use std::mem::replace;
use std::sync::Arc;
use std::sync::Mutex;

/// A boxed up `Observer`.
pub type ObserverBox<T, E> = Box<dyn Observer<T, E> + Send>;

/// A trait for objects that can observe an observable one.
pub trait Observer<T, E>: Debug + Send
where
    T: Send,
    E: Send,
{
    /// Action to perform before data starts coming in from the
    /// Observable.
    fn on_start(&mut self) -> Result<(), E>;

    /// Action to perform when a series of incoming data from the
    /// Observable is committed.
    fn on_commit(&mut self) -> Result<(), E>;

    /// Process a series of incoming items.
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E>;

    /// Action to perform when the `Observable` is about to shut down.
    ///
    /// This method is typically used to clean up any state associated
    /// with the `Observable`.
    fn on_completed(&mut self) -> Result<(), E>;
}

/// Wrapper around an `Observer` that allows for it to be shared by
/// wrapping it into a combination of `Arc` & `Mutex`.
#[derive(Debug)]
pub struct SharedObserver<O>(pub Arc<Mutex<O>>);

impl<O> SharedObserver<O> {
    /// Create a new `SharedObserver` object, automatically wrapping the
    /// provided `Observer` as necessary.
    pub fn new(observer: O) -> Self {
        SharedObserver(Arc::new(Mutex::new(observer)))
    }
}

impl<O> Clone for SharedObserver<O> {
    fn clone(&self) -> Self {
        SharedObserver(self.0.clone())
    }
}

impl<O, T, E> Observer<T, E> for SharedObserver<O>
where
    O: Observer<T, E>,
    T: Send,
    E: Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        self.0.lock().unwrap().on_start()
    }

    fn on_commit(&mut self) -> Result<(), E> {
        self.0.lock().unwrap().on_commit()
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        self.0.lock().unwrap().on_updates(updates)
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.0.lock().unwrap().on_completed()
    }
}

/// Wrapper around an `Observer` that stores updates and pushes them
/// forward only when an `on_commit` is received.
#[derive(Debug)]
pub struct CachingObserver<O, T> {
    /// The observer we ultimately push our data to when we received the
    /// `on_commit` event.
    observer: O,
    /// The data we accumulated so far.
    data: Option<LinkedList<Vec<T>>>,
}

impl<O, T> CachingObserver<O, T> {
    /// Create a new `CachingObserver` wrapping the provided observer.
    pub fn new(observer: O) -> Self {
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
            self.observer.on_start()?;
            let updates_list = replace(data, LinkedList::new());
            for updates in updates_list.into_iter() {
                self.observer.on_updates(Box::new(updates.into_iter()))?;
            }
            self.observer.on_commit()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test::MockObserver;

    /// Test caching of transactions via a `CachingObserver`.
    #[test]
    fn transaction_caching() {
        let mock = SharedObserver::new(MockObserver::new());
        let observer = &mut CachingObserver::new(mock.clone()) as &mut dyn Observer<_, ()>;

        assert_eq!(observer.on_start(), Ok(()));
        assert_eq!(mock.0.lock().unwrap().called_on_start, 0);

        assert_eq!(observer.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(mock.0.lock().unwrap().called_on_updates, 0);

        assert_eq!(observer.on_updates(Box::new([6, 4, 5].iter())), Ok(()));
        assert_eq!(mock.0.lock().unwrap().called_on_updates, 0);

        assert_eq!(observer.on_commit(), Ok(()));
        assert_eq!(mock.0.lock().unwrap().called_on_start, 1);
        assert_eq!(mock.0.lock().unwrap().called_on_updates, 6);
        assert_eq!(mock.0.lock().unwrap().called_on_commit, 1);
    }
}
