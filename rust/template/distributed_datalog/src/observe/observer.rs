use std::fmt::Debug;
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

/// An easily sharable `Observer`.
pub type SharedObserver<O> = Arc<Mutex<O>>;

impl<O, T, E> Observer<T, E> for SharedObserver<O>
where
    O: Observer<T, E>,
    T: Send,
    E: Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        self.lock().unwrap().on_start()
    }

    fn on_commit(&mut self) -> Result<(), E> {
        self.lock().unwrap().on_commit()
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        self.lock().unwrap().on_updates(updates)
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.lock().unwrap().on_completed()
    }
}

/// An optional `Observer`. If set to `None` all events will just be
/// dropped.
pub type OptionalObserver<O> = Option<O>;

impl<O, T, E> Observer<T, E> for OptionalObserver<O>
where
    O: Observer<T, E>,
    T: Send,
    E: Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        self.as_mut().map_or(Ok(()), Observer::on_start)
    }

    fn on_commit(&mut self) -> Result<(), E> {
        self.as_mut().map_or(Ok(()), Observer::on_commit)
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        self.as_mut().map_or(Ok(()), |o| o.on_updates(updates))
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.as_mut().map_or(Ok(()), Observer::on_completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::observe::test::MockObserver;

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
        let mock = Arc::new(Mutex::new(MockObserver::new()));
        let observer = &mut Some(mock.clone()) as &mut dyn Observer<_, ()>;

        assert_eq!(observer.on_start(), Ok(()));
        assert_eq!(mock.lock().unwrap().called_on_start, 1);

        assert_eq!(observer.on_updates(Box::new([1, 3, 2].iter())), Ok(()));
        assert_eq!(mock.lock().unwrap().called_on_updates, 3);

        assert_eq!(observer.on_commit(), Ok(()));
        assert_eq!(mock.lock().unwrap().called_on_commit, 1);
    }
}
