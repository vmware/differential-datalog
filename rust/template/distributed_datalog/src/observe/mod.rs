//! A publish-subscribe infrastructure for differential-datalog programs.

mod observable;
mod observer;
#[cfg(any(test, feature = "test"))]
mod test;

pub use observable::Observable;
pub use observable::ObservableAny;
pub use observable::ObservableBox;
pub use observable::SharedObservable;
pub use observable::UpdatesObservable;
pub use observer::Observer;
pub use observer::ObserverBox;
pub use observer::OptionalObserver;
pub use observer::SharedObserver;

#[cfg(any(test, feature = "test"))]
pub use test::MockObserver;
