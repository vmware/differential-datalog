use std::sync::Arc;
use std::sync::Mutex;

/// A trait for objects that can observe an observable one.
pub trait Observer<T, E>: Send
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

    /// Process an incoming item.
    fn on_next(&mut self, item: T) -> Result<(), E>;

    /// Process a series of incoming items.
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        for upd in updates {
            self.on_next(upd)?;
        }
        Ok(())
    }

    /// Action to perform when the Observable finishes sending
    /// data.
    fn on_completed(&mut self) -> Result<(), E>;

    /// Action to perform when any error occurs
    fn on_error(&self, error: E);
}

/// Wrapper around an `Observer` that allows for it to be shared by
/// wrapping it into a combination of `Arc` & `Mutex`.
#[derive(Debug, Clone)]
pub struct SharedObserver<O>(pub Arc<Mutex<O>>);

impl<O> SharedObserver<O> {
    /// Create a new `SharedObserver` object, automatically wrapping the
    /// provided `Observer` as necessary.
    pub fn new(observer: O) -> Self {
        SharedObserver(Arc::new(Mutex::new(observer)))
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

    fn on_next(&mut self, upd: T) -> Result<(), E> {
        self.0.lock().unwrap().on_next(upd)
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        self.0.lock().unwrap().on_updates(updates)
    }

    fn on_error(&self, error: E) {
        self.0.lock().unwrap().on_error(error)
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.0.lock().unwrap().on_completed()
    }
}
