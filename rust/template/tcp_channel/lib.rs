//! TCP implementation of an Observer/Observable channel.

#![allow(clippy::type_complexity)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs
)]

mod receiver;
mod sender;

pub use receiver::TcpReceiver;
pub use sender::TcpSender;
