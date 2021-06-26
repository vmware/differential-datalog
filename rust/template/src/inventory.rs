use crate::{
    d3log_localize_val, idxkey_from_record, indexes2arrid, indexid2name, rel_name2orig_name,
    relid2name, relkey_from_record, relval_from_record, Indexes, Relations,
    RAW_INPUT_RELATION_ID_MAP,
};
#[cfg(feature = "c_api")]
use crate::{indexid2cname, rel_name2orig_cname, relid2cname};
use differential_datalog::{
    ddval::DDValue,
    program::{ArrId, IdxId, RelId},
    record::{Record, RelIdentifier},
    D3logLocalizer, D3logLocationId, DDlogInventory,
};
use fnv::FnvHashMap;
#[cfg(feature = "c_api")]
use std::ffi::CStr;
use std::{any::TypeId, convert::TryFrom};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Inventory;

impl DDlogInventory for Inventory {
    fn get_table_id(&self, table_name: &str) -> Result<RelId, String> {
        Relations::try_from(table_name).map_or_else(
            |()| Err(format!("unknown relation {}", table_name)),
            |rel| Ok(rel as RelId),
        )
    }

    fn get_table_name(&self, table_id: RelId) -> Result<&'static str, String> {
        relid2name(table_id).ok_or_else(|| format!("unknown relation {}", table_id))
    }

    fn get_table_original_name(&self, table_name: &str) -> Result<&'static str, String> {
        rel_name2orig_name(table_name).ok_or_else(|| format!("unknown relation {}", table_name))
    }

    #[cfg(feature = "c_api")]
    fn get_table_original_cname(&self, table_name: &str) -> Result<&'static CStr, String> {
        rel_name2orig_cname(table_name).ok_or_else(|| format!("unknown relation {}", table_name))
    }

    #[cfg(feature = "c_api")]
    fn get_table_cname(&self, table_id: RelId) -> Result<&'static CStr, String> {
        relid2cname(table_id).ok_or_else(|| format!("unknown relation {}", table_id))
    }

    fn get_index_id(&self, index_name: &str) -> Result<IdxId, String> {
        Indexes::try_from(index_name).map_or_else(
            |()| Err(format!("unknown index {}", index_name)),
            |idx| Ok(idx as IdxId),
        )
    }

    fn get_index_name(&self, index_id: IdxId) -> Result<&'static str, String> {
        indexid2name(index_id).ok_or_else(|| format!("unknown index {}", index_id))
    }

    #[cfg(feature = "c_api")]
    fn get_index_cname(&self, index_id: IdxId) -> Result<&'static CStr, String> {
        indexid2cname(index_id).ok_or_else(|| format!("unknown index {}", index_id))
    }

    fn input_relation_ids(&self) -> &'static FnvHashMap<RelId, &'static str> {
        &*RAW_INPUT_RELATION_ID_MAP
    }

    fn index_from_record(&self, index_id: IdxId, key: &Record) -> Result<DDValue, String> {
        let index =
            Indexes::try_from(index_id).map_err(|_| format!("Unknown index {}", index_id))?;
        idxkey_from_record(index, key)
    }

    fn relation_type_id(&self, relation: RelId) -> Option<TypeId> {
        Relations::try_from(relation)
            .map(|relation| relation.type_id())
            .ok()
    }

    fn relation_value_from_record(
        &self,
        relation: &RelIdentifier,
        record: &Record,
    ) -> Result<(RelId, DDValue), String> {
        let relation = Relations::try_from(relation)
            .map_err(|_| format!("Unknown relation {:?}", relation))?;
        relval_from_record(relation, record).map(|value| (relation as RelId, value))
    }

    fn relation_key_from_record(
        &self,
        relation: &RelIdentifier,
        record: &Record,
    ) -> Result<(RelId, DDValue), String> {
        let relation = Relations::try_from(relation)
            .map_err(|_| format!("Unknown relation {:?}", relation))?;
        relkey_from_record(relation, record).map(|key| (relation as RelId, key))
    }

    fn index_to_arrangement_id(&self, index: IdxId) -> Option<ArrId> {
        let index = Indexes::try_from(index).ok()?;
        Some(indexes2arrid(index))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct D3logInventory;

impl D3logLocalizer for D3logInventory {
    fn localize_value(
        &self,
        relation: RelId,
        value: DDValue,
    ) -> Result<(Option<D3logLocationId>, RelId, DDValue), DDValue> {
        d3log_localize_val(relation, value)
    }
}
