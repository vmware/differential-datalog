#![allow(
    clippy::unknown_clippy_lints,
    clippy::get_unwrap,
    clippy::missing_safety_doc,
    clippy::type_complexity
)]

mod callback;
mod ddlog;
mod profile;
mod profile_statistics;
mod replay;
mod valmap;
mod variable;

pub mod arcval;

#[macro_use]
pub mod ddval;
pub mod int;
pub mod program;
pub mod uint;

#[macro_use]
pub mod record;

pub mod test_value;

#[cfg(test)]
mod test;

#[cfg(test)]
mod test_record;

pub use callback::Callback;
pub use ddlog::DDlog;
pub use ddlog::DDlogConvert;
pub use replay::record_upd_cmds;
pub use replay::record_val_upds;
pub use replay::RecordReplay;
pub use valmap::DeltaMap;
