#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs
)]

//! A crate providing a publish-subscribe infrastructure for
//! differential datalog program.

mod observable;
mod observer;
mod subscription;

pub use observable::Observable;
pub use observer::Observer;
pub use observer::SharedObserver;
pub use subscription::Subscription;
