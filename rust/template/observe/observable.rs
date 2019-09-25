use std::fmt::Debug;

use super::Observer;
use super::Subscription;

/// A trait for objects that can be observed.
pub trait Observable<T, E>: Debug
where
    T: Send,
    E: Send,
{
    /// An Observer subscribes to an Observable to listen to data
    /// emitted by the latter. The Observer stops listening when
    /// the Subscription returned is canceled.
    fn subscribe(&mut self, observer: Box<dyn Observer<T, E> + Send>) -> Box<dyn Subscription>;
}
