#![allow(
    path_statements,
    unused_imports,
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals,
    unused_parens,
    non_shorthand_field_patterns,
    dead_code,
    overflowing_literals,
    unreachable_patterns,
    unused_variables,
    clippy::missing_safety_doc,
    clippy::match_single_binding,
    clippy::ptr_arg,
    clippy::redundant_closure,
    clippy::needless_lifetimes,
    clippy::borrowed_box,
    clippy::map_clone,
    clippy::toplevel_ref_arg,
    clippy::double_parens,
    clippy::collapsible_if,
    clippy::clone_on_copy,
    clippy::unused_unit,
    clippy::deref_addrof,
    clippy::clone_on_copy,
    clippy::needless_return,
    clippy::op_ref,
    clippy::match_like_matches_macro,
    clippy::comparison_chain,
    clippy::len_zero,
    clippy::extra_unused_lifetimes
)]

use std::ops::Deref;

use differential_dataflow::collection;
use timely::communication;
use timely::dataflow::scopes;
use timely::worker;

use ddlog_derive::{FromRecord, IntoRecord, Mutator};
use differential_datalog::ddval::DDValConvert;
use differential_datalog::program;
use differential_datalog::program::TupleTS;
use differential_datalog::program::Weight;
use differential_datalog::program::XFormArrangement;
use differential_datalog::program::XFormCollection;
use differential_datalog::record::FromRecord;
use differential_datalog::record::FromRecordInner;
use differential_datalog::record::IntoRecord;
use differential_datalog::record::Mutator;
use differential_datalog::record::MutatorInner;
use serde::Deserialize;
use serde::Serialize;

// `usize` and `isize` are builtin Rust types; we therefore declare an alias to DDlog's `usize` and
// `isize`.
pub type std_usize = u64;
pub type std_isize = i64;

/*- !!!!!!!!!!!!!!!!!!!! -*/
// Don't edit this line
// Code below this point is needed to test-compile template
// code and is not part of the template.

/* Import bits of DDlog runtime required by `differential_datalog_test` and the `main_crate` test. */
#[path = "../../../lib/ddlog_rt.rs"]
pub mod ddlog_rt;

#[path = "../../../lib/ddlog_bigint.rs"]
pub mod ddlog_bigint;

#[path = "../../../lib/ddlog_log.rs"]
pub mod ddlog_log;
