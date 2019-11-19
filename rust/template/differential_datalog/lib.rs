#![allow(
    clippy::unknown_clippy_lints,
    clippy::get_unwrap,
    clippy::missing_safety_doc,
    clippy::type_complexity
)]

mod callback;
mod ddlog;
mod profile;
mod replay;
#[cfg(test)]
mod test;
mod valmap;
mod variable;

pub mod arcval;
pub mod int;
pub mod program;
pub mod uint;

#[macro_use]
pub mod record;

#[cfg(test)]
mod test_record;

pub use callback::Callback;
pub use ddlog::DDlog;
pub use ddlog::DDlogConvert;
pub use replay::record_upd_cmds;
pub use replay::record_val_upds;
pub use replay::RecordReplay;
pub use valmap::DeltaMap;
