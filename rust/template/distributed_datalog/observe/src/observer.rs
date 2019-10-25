use std::fmt::Debug;
use std::ops::Deref;
use std::ops::DerefMut;
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

// We need a direct implementation of `Observer` for boxed up observers
// or Rust will complain in all sorts of undecipherable ways when we end
// up with nested `ObserverBox`s.
impl<T, E, O> Observer<T, E> for Box<O>
where
    T: Send,
    E: Send,
    O: Observer<T, E> + ?Sized,
{
    fn on_start(&mut self) -> Result<(), E> {
        self.deref_mut().on_start()
    }

    fn on_commit(&mut self) -> Result<(), E> {
        self.deref_mut().on_commit()
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        self.deref_mut().on_updates(updates)
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.deref_mut().on_completed()
    }
}

/// Wrapper around an `Observer` that allows for it to be shared by
/// wrapping it into a combination of `Arc` & `Mutex`.
#[derive(Debug, Default)]
pub struct SharedObserver<O>(Arc<Mutex<O>>);

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

impl<O> Deref for SharedObserver<O> {
    type Target = Arc<Mutex<O>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<O> DerefMut for SharedObserver<O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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

/// An `Observer` that wraps an inner, optional `Observer`. If the inner
/// one is unset all events will just be dropped.
#[derive(Debug)]
pub struct OptionalObserver<O>(Option<O>);

impl<O> OptionalObserver<O> {
    /// Create a new `OptionalObserver` object, automatically wrapping the
    /// provided `Observer` as necessary.
    pub fn new(observer: O) -> Self {
        Self(Some(observer))
    }

    /// Replace the existing `Observer` (if any) with the given optional
    /// one, returning the previous one.
    pub fn replace(&mut self, value: O) -> Option<O> {
        self.0.replace(value)
    }

    /// Take the inner observer, if any, replacing it with none.
    pub fn take(&mut self) -> Option<O> {
        self.0.take()
    }

    /// Check whether an actual observer is present or not.
    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }

    /// Retrieve an `Option` referencing the inner `Observer`.
    pub fn as_ref(&self) -> Option<&O> {
        self.0.as_ref()
    }
}

impl<O> Default for OptionalObserver<O> {
    fn default() -> Self {
        Self(None)
    }
}

impl<O, T, E> Observer<T, E> for OptionalObserver<O>
where
    O: Observer<T, E>,
    T: Send,
    E: Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        self.0.as_mut().map_or(Ok(()), Observer::on_start)
    }

    fn on_commit(&mut self) -> Result<(), E> {
        self.0.as_mut().map_or(Ok(()), Observer::on_commit)
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        self.0.as_mut().map_or(Ok(()), |o| o.on_updates(updates))
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.0.as_mut().map_or(Ok(()), Observer::on_completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test::MockObserver;

    /// Test the workings of an `OptionalObserver` with no actual
    /// observer present.
    #[test]
    fn no_optional_observer() {
        let observer = &mut OptionalObserver::<MockObserver>::default() as &mut dyn Observer<_, ()>;

        assert_eq!(observer.on_start(), Ok(()));
        assert_eq!(observer.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(observer.on_commit(), Ok(()));
    }

    /// Test the workings of an `OptionalObserver` with an observer
    /// present.
    #[test]
    fn with_optional_observer() {
        let mock = SharedObserver::new(MockObserver::new());
        let observer = &mut OptionalObserver::new(mock.clone()) as &mut dyn Observer<_, ()>;

        assert_eq!(observer.on_start(), Ok(()));
        assert_eq!(mock.0.lock().unwrap().called_on_start, 1);

        assert_eq!(observer.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(mock.0.lock().unwrap().called_on_updates, 3);

        assert_eq!(observer.on_commit(), Ok(()));
        assert_eq!(mock.0.lock().unwrap().called_on_commit, 1);
    }
}
