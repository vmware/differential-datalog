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
    clippy::unknown_clippy_lints,
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

use num::bigint::BigInt;
use std::convert::TryFrom;
use std::hash::Hash;
use std::ops::Deref;
use std::ptr;
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
use differential_datalog::DDlogConvert;
use num_traits::cast::FromPrimitive;
use num_traits::identities::One;
use once_cell::sync::Lazy;

use fnv::FnvHashMap;

pub mod api;
pub mod ovsdb_api;
pub mod update_handler;

use crate::api::updcmd2upd;

use serde::ser::SerializeTuple;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

/// A default implementation of `DDlogConvert` that just forwards calls
/// to generated functions of equal name.
#[derive(Debug)]
pub struct DDlogConverter {}

impl DDlogConvert for DDlogConverter {
    fn relid2name(relId: program::RelId) -> Option<&'static str> {
        relid2name(relId)
    }

    fn indexid2name(idxId: program::IdxId) -> Option<&'static str> {
        indexid2name(idxId)
    }

    fn updcmd2upd(upd_cmd: &UpdCmd) -> ::std::result::Result<program::Update<DDValue>, String> {
        updcmd2upd(upd_cmd)
    }
}

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

impl Serialize for UpdateSerializer {
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
                    where A: ::serde::de::SeqAccess<'de> {
                        let polarity = seq.next_element::<bool>()?.ok_or_else(|| <A::Error as ::serde::de::Error>::custom("Missing polarity"))?;
                        let relid = seq.next_element::<program::RelId>()?.ok_or_else(|| <A::Error as ::serde::de::Error>::custom("Missing relation id"))?;
                        match relid {
                            $(
                                $rel => {
                                    let v = seq.next_element::<$typ>()?.ok_or_else(|| <A::Error as ::serde::de::Error>::custom("Missing value"))?.into_ddvalue();
                                    if polarity {
                                        Ok(UpdateSerializer(program::Update::Insert{relid, v}))
                                    } else {
                                        Ok(UpdateSerializer(program::Update::DeleteValue{relid, v}))
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

/*- !!!!!!!!!!!!!!!!!!!! -*/
// Don't edit this line
// Code below this point is needed to test-compile template
// code and is not part of the template.

/* Import bits of DDlog runtime required by `differential_datalog_test` and the `main_crate` test. */
#[path = "../../../lib/ddlog_bigint.rs"]
pub mod ddlog_bigint;

#[path = "../../../lib/ddlog_log.rs"]
pub mod ddlog_log;

pub fn prog(__update_cb: Box<dyn program::CBFn>) -> program::Program {
    panic!("prog not implemented")
}

impl<'de> Deserialize<'de> for UpdateSerializer {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        panic!("Not implemented: UpdateSerializer::deserialize")
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Relations {
    X = 0,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
impl Relations {
    pub fn is_input(&self) -> bool {
        panic!("Relations::is_input not implemented")
    }

    pub fn is_output(&self) -> bool {
        panic!("Relations::is_output not implemented")
    }

    pub fn type_id(&self) -> TypeId {
        panic!("Relations::type_id not implemented")
    }
}

impl TryFrom<&str> for Relations {
    type Error = ();

    fn try_from(rname: &str) -> ::std::result::Result<Self, ()> {
        panic!("Relations::try_from::<&str> not implemented")
    }
}

impl TryFrom<program::RelId> for Relations {
    type Error = ();

    fn try_from(rid: program::RelId) -> ::std::result::Result<Self, ()> {
        panic!("Relations::try_from::<RelId> not implemented")
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Indexes {
    X = 0,
}

impl TryFrom<&str> for Indexes {
    type Error = ();

    fn try_from(_iname: &str) -> ::std::result::Result<Self, ()> {
        panic!("Indexes::try_from::<&str> not implemented")
    }
}

impl TryFrom<program::IdxId> for Indexes {
    type Error = ();

    fn try_from(_iid: program::IdxId) -> ::std::result::Result<Self, ()> {
        panic!("Indexes::try_from::<program::IdxId> not implemented")
    }
}

pub fn relval_from_record(
    _rel: Relations,
    _rec: &record::Record,
) -> ::std::result::Result<DDValue, String> {
    panic!("relval_from_record not implemented")
}

pub fn relkey_from_record(
    _rel: Relations,
    _rec: &record::Record,
) -> ::std::result::Result<DDValue, String> {
    panic!("relkey_from_record not implemented")
}

pub fn idxkey_from_record(
    idx: Indexes,
    _rec: &record::Record,
) -> ::std::result::Result<DDValue, String> {
    panic!("idxkey_from_record not implemented")
}

pub fn relid2name(_rid: program::RelId) -> Option<&'static str> {
    panic!("relid2name not implemented")
}

pub fn relid2cname(_rid: program::RelId) -> Option<&'static ::std::ffi::CStr> {
    panic!("relid2cname not implemented")
}

pub static RELIDMAP: Lazy<FnvHashMap<Relations, &'static str>> = Lazy::new(FnvHashMap::default);
pub static INPUT_RELIDMAP: Lazy<FnvHashMap<Relations, &'static str>> =
    Lazy::new(FnvHashMap::default);
pub static OUTPUT_RELIDMAP: Lazy<FnvHashMap<Relations, &'static str>> =
    Lazy::new(FnvHashMap::default);

pub fn indexid2name(_iid: program::IdxId) -> Option<&'static str> {
    panic!("indexid2name not implemented")
}

pub fn indexid2cname(_iid: program::IdxId) -> Option<&'static ::std::ffi::CStr> {
    panic!("indexid2cname not implemented")
}

pub fn indexes2arrid(idx: Indexes) -> program::ArrId {
    panic!("indexes2arrid not implemented")
}

pub static IDXIDMAP: Lazy<FnvHashMap<Indexes, &'static str>> = Lazy::new(FnvHashMap::default);
