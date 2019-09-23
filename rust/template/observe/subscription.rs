/// A trait representing an object subscribed to an `Observable`.
pub trait Subscription {
    /// Cancel the subscription so the corresponding Observer
    /// stops listening to the corresponding Observable.
    fn unsubscribe(self: Box<Self>);
}
