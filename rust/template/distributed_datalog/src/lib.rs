#![allow(clippy::type_complexity)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs
)]

//! Distributed computing for differential-datalog.

mod tcp_channel;
mod txnmux;

pub use observe::Observable;
pub use observe::ObservableBox;
pub use observe::Observer;
pub use observe::ObserverBox;
pub use observe::OptionalObserver;
pub use observe::SharedObserver;
pub use observe::UpdatesObservable;
pub use tcp_channel::TcpReceiver;
pub use tcp_channel::TcpSender;
pub use txnmux::TxnMux;

#[cfg(any(test, feature = "test"))]
pub use observe::MockObserver;
