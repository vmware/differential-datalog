use std::fmt::Debug;
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
