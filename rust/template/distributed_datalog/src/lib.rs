#![allow(clippy::type_complexity)]
#![warn(
    bad_style,
    dead_code,
    future_incompatible,
    illegal_floating_point_literal_pattern,
    improper_ctypes,
    intra_doc_link_resolution_failure,
    late_bound_lifetime_arguments,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    no_mangle_generic_items,
    non_shorthand_field_patterns,
    nonstandard_style,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    plugin_as_library,
    private_in_public,
    proc_macro_derive_resolution_fallback,
    renamed_and_removed_lints,
    rust_2018_compatibility,
    rust_2018_idioms,
    safe_packed_borrows,
    stable_features,
    trivial_bounds,
    trivial_numeric_casts,
    type_alias_bounds,
    tyvar_behind_raw_pointer,
    unconditional_recursion,
    unions_with_drop_fields,
    unreachable_code,
    unreachable_patterns,
    unstable_features,
    unstable_name_collisions,
    unused,
    unused_comparisons,
    unused_import_braces,
    unused_lifetimes,
    unused_qualifications,
    unused_results,
    where_clauses_object_safety,
    while_true
)]

//! Distributed computing for differential-datalog.

mod instantiate;
mod members;
mod observe;
mod schema;
mod server;
mod tcp_channel;
#[cfg(any(test, feature = "test"))]
mod test;
mod txnmux;
mod zookeeper;

/// A module comprising sinks to forward data from a computation.
pub mod sinks;

/// A module comprising sources to feed data into a computation.
pub mod sources;

pub use instantiate::instantiate;
pub use instantiate::simple_assign;
pub use observe::Observable;
pub use observe::ObservableBox;
pub use observe::Observer;
pub use observe::ObserverBox;
pub use observe::OptionalObserver;
pub use observe::SharedObserver;
pub use observe::UpdatesObservable;
pub use schema::Addr;
pub use schema::Member;
pub use schema::Members;
pub use schema::Node;
pub use schema::NodeCfg;
pub use schema::RelCfg;
pub use schema::Relations;
pub use schema::Sink;
pub use schema::Source;
pub use schema::SysCfg;
pub use server::DDlogServer;
pub use tcp_channel::TcpReceiver;
pub use tcp_channel::TcpSender;
pub use txnmux::TxnMux;

#[cfg(any(test, feature = "test"))]
pub use observe::MockObserver;

#[cfg(any(test, feature = "test"))]
pub use test::await_expected;
