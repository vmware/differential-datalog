#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs
)]

//! A crate providing a publish-subscribe infrastructure for
//! differential datalog program.

mod observable;
mod observer;
#[cfg(any(test, feature = "test"))]
mod test;

pub use observable::Observable;
pub use observable::ObservableAny;
pub use observable::ObservableBox;
pub use observable::UpdatesObservable;
pub use observer::Observer;
pub use observer::ObserverBox;
pub use observer::OptionalObserver;
pub use observer::SharedObserver;

#[cfg(feature = "test")]
pub use test::MockObserver;
