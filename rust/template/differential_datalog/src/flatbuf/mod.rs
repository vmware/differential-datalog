//! Serialize DDlog commands to/from FlatBuffers

use crate::{
    ddval::DDValue,
    program::{IdxId, Update},
    DeltaMap,
};
use std::collections::BTreeSet;

pub trait FlatbufConverter {
    fn updates_from_buffer(&self, buffer: &[u8]) -> Result<Vec<Update<DDValue>>, String>;

    fn query_index_from_buffer(&self, buffer: &[u8]) -> Result<(IdxId, DDValue), String>;

    fn updates_to_buffer(&self, updates: &DeltaMap<DDValue>) -> Result<(Vec<u8>, usize), String>;

    fn index_values_to_buffer(
        &self,
        index_id: IdxId,
        contents: &BTreeSet<DDValue>,
    ) -> Result<(Vec<u8>, usize), String>;
}

/// A no-op flatbuffers converter that returns an error on all operations
#[derive(Debug, Clone)]
pub struct UnimplementedFlatbufConverter;

impl FlatbufConverter for UnimplementedFlatbufConverter {
    fn updates_from_buffer(&self, _buffer: &[u8]) -> Result<Vec<Update<DDValue>>, String> {
        Err("DDlog was compiled without FlatBuffers support".to_owned())
    }

    fn query_index_from_buffer(&self, _buffer: &[u8]) -> Result<(IdxId, DDValue), String> {
        Err("DDlog was compiled without FlatBuffers support".to_owned())
    }

    fn updates_to_buffer(&self, _updates: &DeltaMap<DDValue>) -> Result<(Vec<u8>, usize), String> {
        Err("DDlog was compiled without FlatBuffers support".to_owned())
    }

    fn index_values_to_buffer(
        &self,
        _index_id: IdxId,
        _contents: &BTreeSet<DDValue>,
    ) -> Result<(Vec<u8>, usize), String> {
        Err("DDlog was compiled without FlatBuffers support".to_owned())
    }
}
