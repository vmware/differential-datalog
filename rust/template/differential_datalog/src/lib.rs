#![allow(
    clippy::get_unwrap,
    clippy::missing_safety_doc,
    clippy::type_complexity
)]

pub mod api;
mod callback;
mod dataflow;
mod ddlog;
pub mod flatbuf;
mod profile;
mod profile_statistics;
mod render;
pub mod replay;
mod valmap;
mod variable;

#[macro_use]
pub mod ddval;
pub mod program;

#[macro_use]
pub mod record;

#[cfg(test)]
mod test_record;

pub use callback::Callback;
pub use ddlog::{
    AnyDeserialize, AnyDeserializeFunc, D3log, D3logLocalizer, D3logLocationId, DDlog, DDlogDump,
    DDlogDynamic, DDlogInventory, DDlogProfiling,
};
pub use replay::CommandRecorder;
pub use triomphe;
pub use valmap::DeltaMap;
