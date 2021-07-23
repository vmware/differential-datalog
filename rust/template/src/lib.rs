#![allow(
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
    clippy::toplevel_ref_arg,
    clippy::double_parens,
    clippy::clone_on_copy,
    clippy::just_underscores_and_digits,
    clippy::match_single_binding,
    clippy::op_ref,
    clippy::nonminimal_bool,
    clippy::redundant_clone
)]

mod inventory;
pub mod ovsdb_api;

pub use inventory::{D3logInventory, Inventory};

use num::bigint::BigInt;
use std::convert::TryFrom;
use std::hash::Hash;
use std::ops::Deref;
use std::result;
use std::{any::TypeId, sync};

use ordered_float::*;

use differential_dataflow::collection;
use timely::communication;
use timely::dataflow::scopes;
use timely::worker;

use differential_datalog::ddval::*;
use differential_datalog::program;
use differential_datalog::record;
use differential_datalog::record::FromRecord;
use differential_datalog::record::IntoRecord;
use differential_datalog::record::RelIdentifier;
use differential_datalog::record::UpdCmd;
use differential_datalog::D3logLocationId;
use num_traits::cast::FromPrimitive;
use num_traits::identities::One;
use once_cell::sync::Lazy;

use fnv::{FnvBuildHasher, FnvHashMap};

use serde::ser::SerializeTuple;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

// This import is only needed to convince the OS X compiler to export
// `extern C` functions declared in ddlog_log.rs in the generated lib.
#[doc(hidden)]
#[cfg(feature = "c_api")]
pub use ddlog_log as hidden_ddlog_log;
#[doc(hidden)]
#[cfg(feature = "c_api")]
pub use differential_datalog::api as hidden_ddlog_api;

/* Wrapper around `Update<DDValue>` type that implements `Serialize` and `Deserialize`
 * traits.  It is currently only used by the distributed_ddlog crate in order to
 * serialize updates before sending them over the network and deserializing them on the
 * way back.  In other scenarios, the user either creates a `Update<DDValue>` type
 * themselves (when using the strongly typed DDlog API) or deserializes `Update<DDValue>`
 * from `Record` using `DDlogConvert::updcmd2upd()`.
 *
 * Why use a wrapper instead of implementing the traits for `Update<DDValue>` directly?
 * `Update<>` and `DDValue` types are both declared in the `differential_datalog` crate,
 * whereas the `Deserialize` implementation is program-specific and must be in one of the
 * generated crates, so we need a wrapper to avoid creating an orphan `impl`.
 *
 * Serialized representation: we currently only serialize `Insert` and `DeleteValue`
 * commands, represented in serialized form as (polarity, relid, value) tuple.  This way
 * the deserializer first reads relid and uses it to decide which value to deserialize
 * next.
 *
 * `impl Serialize` - serializes the value by forwarding `serialize` call to the `DDValue`
 * object (in fact, it is generic and could be in the `differential_datalog` crate, but we
 * keep it here to make it easier to keep it in sync with `Deserialize`).
 *
 * `impl Deserialize` - gets generated in `Compile.hs` using the macro below.  The macro
 * takes a list of `(relid, type)` and generates a match statement that uses type-specific
 * `Deserialize` for each `relid`.
 */
#[derive(Debug)]
pub struct UpdateSerializer(program::Update<DDValue>);

impl From<program::Update<DDValue>> for UpdateSerializer {
    fn from(u: program::Update<DDValue>) -> Self {
        UpdateSerializer(u)
    }
}
impl From<UpdateSerializer> for program::Update<DDValue> {
    fn from(u: UpdateSerializer) -> Self {
        u.0
    }
}

impl ::serde::Serialize for UpdateSerializer {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tup = serializer.serialize_tuple(3)?;
        match &self.0 {
            program::Update::Insert { relid, v } => {
                tup.serialize_element(&true)?;
                tup.serialize_element(relid)?;
                tup.serialize_element(v)?;
            }
            program::Update::DeleteValue { relid, v } => {
                tup.serialize_element(&false)?;
                tup.serialize_element(relid)?;
                tup.serialize_element(v)?;
            }
            _ => panic!("Cannot serialize InsertOrUpdate/Modify/DeleteKey update"),
        };
        tup.end()
    }
}

#[macro_export]
macro_rules! decl_update_deserializer {
    ( $n:ty, $(($rel:expr, $typ:ty)),* ) => {
        impl<'de> ::serde::Deserialize<'de> for $n {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> ::std::result::Result<Self, D::Error> {
                struct UpdateVisitor;

                impl<'de> ::serde::de::Visitor<'de> for UpdateVisitor {
                    type Value = $n;

                    fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                        formatter.write_str("(polarity, relid, value) tuple")
                    }

                    fn visit_seq<A>(self, mut seq: A) -> ::std::result::Result<Self::Value, A::Error>
                    where
                        A: ::serde::de::SeqAccess<'de>,
                    {
                        let polarity = seq.next_element::<bool>()?.ok_or_else(|| <A::Error as ::serde::de::Error>::custom("Missing polarity"))?;
                        let relid = seq.next_element::<::differential_datalog::program::RelId>()?.ok_or_else(|| <A::Error as ::serde::de::Error>::custom("Missing relation id"))?;
                        match relid {
                            $(
                                $rel => {
                                    let v = seq.next_element::<$typ>()?.ok_or_else(|| <A::Error as ::serde::de::Error>::custom("Missing value"))?.into_ddvalue();
                                    if polarity {
                                        Ok(UpdateSerializer(::differential_datalog::program::Update::Insert { relid, v }))
                                    } else {
                                        Ok(UpdateSerializer(::differential_datalog::program::Update::DeleteValue { relid, v }))
                                    }
                                },
                            )*
                            _ => {
                                ::std::result::Result::Err(<A::Error as ::serde::de::Error>::custom(format!("Unknown input relation id {}", relid)))
                            }
                        }
                    }
                }

                deserializer.deserialize_tuple(3, UpdateVisitor)
            }
        }
    };
}

/* FlatBuffers bindings generated by `ddlog` */
#[cfg(feature = "flatbuf")]
pub mod flatbuf;

#[cfg(feature = "flatbuf")]
pub mod flatbuf_generated;

impl TryFrom<&RelIdentifier> for Relations {
    type Error = ();

    fn try_from(rel_id: &RelIdentifier) -> ::std::result::Result<Self, ()> {
        match rel_id {
            RelIdentifier::RelName(rname) => Relations::try_from(rname.as_ref()),
            RelIdentifier::RelId(id) => Relations::try_from(*id),
        }
    }
}

// Macro used to implement `trait D3log`. Invoked from generated code.
#[macro_export]
macro_rules! impl_trait_d3log {
    () => {
        fn d3log_localize_val(
            _relid: ::differential_datalog::program::RelId,
            value: ::differential_datalog::ddval::DDValue,
        ) -> ::core::result::Result<
            (
                ::core::option::Option<::differential_datalog::D3logLocationId>,
                ::differential_datalog::program::RelId,
                ::differential_datalog::ddval::DDValue,
            ),
            ::differential_datalog::ddval::DDValue,
        > {
            ::core::result::Result::Err(value)
        }
    };

    ( $(($out_rel:expr, $in_rel:expr, $typ:ty)),+ ) => {
        pub static D3LOG_CONVERTER_MAP: ::once_cell::sync::Lazy<::std::collections::HashMap<program::RelId, fn(DDValue)->Result<(Option<D3logLocationId>, program::RelId, DDValue), DDValue>>> = ::once_cell::sync::Lazy::new(|| {
            let mut m = ::std::collections::HashMap::new();
            $(
                m.insert($out_rel, { fn __f(val: DDValue) -> Result<(Option<D3logLocationId>, program::RelId, DDValue), DDValue> {
                    if let Some(::ddlog_std::tuple2(loc_id, inner_val)) = <::ddlog_std::tuple2<ddlog_std::Option<D3logLocationId>, $typ>>::try_from_ddvalue_ref(&val) {
                        Ok((::ddlog_std::std2option(*loc_id), $in_rel, (*inner_val).clone().into_ddvalue()))
                    } else {
                        Err(val)
                    }
                } __f as fn(DDValue)->Result<(Option<D3logLocationId>, program::RelId, DDValue), DDValue>});
            )*
            m
        });
        fn d3log_localize_val(relid: program::RelId, val: DDValue) -> Result<(Option<D3logLocationId>, program::RelId, DDValue), DDValue> {
            if let Some(f) = D3LOG_CONVERTER_MAP.get(&relid) {
                f(val)
            } else {
                Err(val)
            }
        }
    };
}

static RAW_RELATION_ID_MAP: ::once_cell::sync::Lazy<
    ::fnv::FnvHashMap<::differential_datalog::program::RelId, &'static ::core::primitive::str>,
> = ::once_cell::sync::Lazy::new(|| {
    let mut map = ::fnv::FnvHashMap::with_capacity_and_hasher(
        crate::RELIDMAP.len(),
        ::fnv::FnvBuildHasher::default(),
    );

    for (&relation, &name) in crate::RELIDMAP.iter() {
        map.insert(relation as ::differential_datalog::program::RelId, name);
    }

    map
});

static RAW_INPUT_RELATION_ID_MAP: ::once_cell::sync::Lazy<
    ::fnv::FnvHashMap<::differential_datalog::program::RelId, &'static ::core::primitive::str>,
> = ::once_cell::sync::Lazy::new(|| {
    let mut map = ::fnv::FnvHashMap::with_capacity_and_hasher(
        crate::INPUT_RELIDMAP.len(),
        ::fnv::FnvBuildHasher::default(),
    );

    for (&relation, &name) in crate::INPUT_RELIDMAP.iter() {
        map.insert(relation as ::differential_datalog::program::RelId, name);
    }

    map
});

/// Create an instance of the DDlog program.
///
/// `config` - program configuration.
/// `do_store` - indicates whether DDlog will track the complete snapshot
///   of output relations.  Should only be used for debugging in order to dump
///   the contents of output tables using `HDDlog::dump_table()`.  Otherwise,
///   indexes are the preferred way to achieve this.
///
/// Returns a handle to the program and initial contents of output relations.
pub fn run_with_config(
    config: ::differential_datalog::program::config::Config,
    do_store: bool,
) -> Result<
    (
        ::differential_datalog::api::HDDlog,
        ::differential_datalog::DeltaMap<DDValue>,
    ),
    String,
> {
    #[cfg(feature = "flatbuf")]
    let flatbuf_converter = Box::new(crate::flatbuf::DDlogFlatbufConverter);
    #[cfg(not(feature = "flatbuf"))]
    let flatbuf_converter = Box::new(differential_datalog::flatbuf::UnimplementedFlatbufConverter);

    ::differential_datalog::api::HDDlog::new(
        config,
        do_store,
        None,
        crate::prog,
        Box::new(crate::Inventory),
        Box::new(crate::D3logInventory),
        flatbuf_converter,
    )
}

/// Create an instance of the DDlog program with default configuration.
///
/// `workers` - number of worker threads (typical values are in the range from 1 to 4).
/// `do_store` - indicates whether DDlog will track the complete snapshot
///   of output relations.  Should only be used for debugging in order to dump
///   the contents of output tables using `HDDlog::dump_table()`.  Otherwise,
///   indexes are the preferred way to achieve this.
///
/// Returns a handle to the program and initial contents of output relations.
pub fn run(
    workers: usize,
    do_store: bool,
) -> Result<
    (
        ::differential_datalog::api::HDDlog,
        ::differential_datalog::DeltaMap<DDValue>,
    ),
    String,
> {
    let config =
        ::differential_datalog::program::config::Config::new().with_timely_workers(workers);

    #[cfg(feature = "flatbuf")]
    let flatbuf_converter = Box::new(crate::flatbuf::DDlogFlatbufConverter);
    #[cfg(not(feature = "flatbuf"))]
    let flatbuf_converter = Box::new(differential_datalog::flatbuf::UnimplementedFlatbufConverter);

    ::differential_datalog::api::HDDlog::new(
        config,
        do_store,
        None,
        crate::prog,
        Box::new(crate::Inventory),
        Box::new(crate::D3logInventory),
        flatbuf_converter,
    )
}

#[no_mangle]
#[cfg(feature = "c_api")]
pub unsafe extern "C" fn ddlog_run_with_config(
    config: *const ::differential_datalog::api::ddlog_config,
    do_store: bool,
    print_err: Option<extern "C" fn(msg: *const ::std::os::raw::c_char)>,
    init_state: *mut *mut ::differential_datalog::DeltaMap<DDValue>,
) -> *const ::differential_datalog::api::HDDlog {
    use ::core::{
        ptr,
        result::Result::{Err, Ok},
    };
    use ::differential_datalog::api::HDDlog;
    use ::std::{boxed::Box, format};
    use ::triomphe::Arc;

    let config = match config.as_ref() {
        None => Default::default(),
        Some(config) => match config.to_rust_api() {
            Ok(cfg) => cfg,
            Err(err) => {
                HDDlog::print_err(
                    print_err,
                    &format!("ddlog_run_with_config(): invalid config: {}", err),
                );
                return ptr::null();
            }
        },
    };

    #[cfg(feature = "flatbuf")]
    let flatbuf_converter = Box::new(crate::flatbuf::DDlogFlatbufConverter);
    #[cfg(not(feature = "flatbuf"))]
    let flatbuf_converter = Box::new(differential_datalog::flatbuf::UnimplementedFlatbufConverter);

    let result = HDDlog::new(
        config,
        do_store,
        print_err,
        crate::prog,
        Box::new(crate::Inventory),
        Box::new(crate::D3logInventory),
        flatbuf_converter,
    );

    match result {
        Ok((hddlog, init)) => {
            if !init_state.is_null() {
                *init_state = Box::into_raw(Box::new(init));
            }
            // Note: This is `triomphe::Arc`, *not* `std::sync::Arc`
            Arc::into_raw(Arc::new(hddlog))
        }
        Err(err) => {
            HDDlog::print_err(print_err, &format!("HDDlog::new() failed: {}", err));
            ptr::null()
        }
    }
}

#[no_mangle]
#[cfg(feature = "c_api")]
pub unsafe extern "C" fn ddlog_run(
    workers: ::std::os::raw::c_uint,
    do_store: bool,
    print_err: Option<extern "C" fn(msg: *const ::std::os::raw::c_char)>,
    init_state: *mut *mut ::differential_datalog::DeltaMap<DDValue>,
) -> *const ::differential_datalog::api::HDDlog {
    let config = ::differential_datalog::api::ddlog_config {
        num_timely_workers: workers,
        ..Default::default()
    };
    ddlog_run_with_config(&config, do_store, print_err, init_state)
}

/*- !!!!!!!!!!!!!!!!!!!! -*/
// Don't edit this line
// Code below this point is needed to test-compile template
// code and is not part of the template.

/* Import bits of DDlog runtime required by `differential_datalog_test` and the `main_crate` test. */
#[path = "../../../lib/ddlog_bigint.rs"]
pub mod ddlog_bigint;

#[path = "../../../lib/ddlog_log.rs"]
pub mod ddlog_log;

pub fn prog(
    __update_cb: ::std::sync::Arc<dyn ::differential_datalog::program::RelationCallback>,
) -> ::differential_datalog::program::Program {
    ::std::panic!("prog not implemented")
}

impl<'de> ::serde::Deserialize<'de> for crate::UpdateSerializer {
    fn deserialize<D: ::serde::de::Deserializer<'de>>(
        deserializer: D,
    ) -> ::std::result::Result<Self, D::Error> {
        ::std::panic!("Not implemented: UpdateSerializer::deserialize")
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Relations {
    X = 0,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
impl Relations {
    pub fn is_input(&self) -> bool {
        ::std::panic!("Relations::is_input not implemented")
    }

    pub fn is_output(&self) -> bool {
        ::std::panic!("Relations::is_output not implemented")
    }

    pub fn type_id(&self) -> TypeId {
        ::std::panic!("Relations::type_id not implemented")
    }
}

impl ::std::convert::TryFrom<&str> for crate::Relations {
    type Error = ();

    fn try_from(rname: &str) -> ::std::result::Result<Self, ()> {
        ::std::panic!("Relations::try_from::<&str> not implemented")
    }
}

impl ::std::convert::TryFrom<program::RelId> for crate::Relations {
    type Error = ();

    fn try_from(rid: ::differential_datalog::program::RelId) -> ::std::result::Result<Self, ()> {
        ::std::panic!("Relations::try_from::<RelId> not implemented")
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Indexes {
    X = 0,
}

impl ::std::convert::TryFrom<&str> for crate::Indexes {
    type Error = ();

    fn try_from(_iname: &str) -> ::std::result::Result<Self, ()> {
        ::std::panic!("Indexes::try_from::<&str> not implemented")
    }
}

impl ::std::convert::TryFrom<::differential_datalog::program::IdxId> for crate::Indexes {
    type Error = ();

    fn try_from(_iid: ::differential_datalog::program::IdxId) -> ::std::result::Result<Self, ()> {
        ::std::panic!("Indexes::try_from::<program::IdxId> not implemented")
    }
}

pub fn relval_from_record(
    _rel: crate::Relations,
    _rec: &::differential_datalog::record::Record,
) -> ::std::result::Result<::differential_datalog::ddval::DDValue, ::std::string::String> {
    ::std::panic!("relval_from_record not implemented")
}

pub fn relkey_from_record(
    _rel: crate::Relations,
    _rec: &::differential_datalog::record::Record,
) -> ::std::result::Result<DDValue, String> {
    ::std::panic!("relkey_from_record not implemented")
}

pub fn idxkey_from_record(
    idx: crate::Indexes,
    _rec: &::differential_datalog::record::Record,
) -> ::std::result::Result<::differential_datalog::ddval::DDValue, ::std::string::String> {
    ::std::panic!("idxkey_from_record not implemented")
}

pub fn relid2name(_rid: ::differential_datalog::program::RelId) -> Option<&'static str> {
    ::std::panic!("relid2name not implemented")
}

pub fn rel_name2orig_name(_tname: &str) -> Option<&'static str> {
    ::std::panic!("rel_name2orig_name not implemented")
}

pub fn relid2cname(_rid: program::RelId) -> Option<&'static ::std::ffi::CStr> {
    ::std::panic!("relid2cname not implemented")
}

pub fn rel_name2orig_cname(_tname: &str) -> Option<&'static ::std::ffi::CStr> {
    ::std::panic!("rel_name2orig_cname not implemented")
}

pub static RELIDMAP: Lazy<FnvHashMap<Relations, &'static str>> = Lazy::new(FnvHashMap::default);
pub static INPUT_RELIDMAP: Lazy<FnvHashMap<Relations, &'static str>> =
    Lazy::new(FnvHashMap::default);
pub static OUTPUT_RELIDMAP: Lazy<FnvHashMap<Relations, &'static str>> =
    Lazy::new(FnvHashMap::default);

pub fn indexid2name(_iid: program::IdxId) -> Option<&'static str> {
    ::std::panic!("indexid2name not implemented")
}

pub fn indexid2cname(_iid: program::IdxId) -> Option<&'static ::std::ffi::CStr> {
    ::std::panic!("indexid2cname not implemented")
}

pub fn indexes2arrid(idx: Indexes) -> program::ArrId {
    ::std::panic!("indexes2arrid not implemented")
}

pub static IDXIDMAP: Lazy<FnvHashMap<Indexes, &'static str>> = Lazy::new(FnvHashMap::default);

impl_trait_d3log!();
