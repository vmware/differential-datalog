#[warn(missing_docs)]

/// A trait for objects that can be observed.
pub trait Observable<T, E>
where
    T: Send,
    E: Send,
{
    /// An Observer subscribes to an Observable to listen to data
    /// emitted by the latter. The Observer stops listening when
    /// the Subscription returned is canceled.
    fn subscribe(&mut self, observer: Box<dyn Observer<T, E> + Send>) -> Box<dyn Subscription>;
}

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

pub trait Subscription {
    /// Cancel the subscription so the corresponding Observer
    /// stops listening to the corresponding Observable.
    fn unsubscribe(self: Box<Self>);
}
