use std::iter::FromIterator;
pub use ddlog_ovsdb_adapter::ovsdb_uuid2str;

pub fn ovsdb_map_extract_val_uuids<K: Val>(ids: &std_Map<K, ovsdb_uuid_or_string_t>) -> std_Map<K, ovsdb_uuid> {
    std_Map::from_iter(ids.x.iter().map(|(k,v)| (k.clone(), ovsdb_extract_uuid(v))))
}
pub fn ovsdb_set_extract_uuids(ids: &std_Set<ovsdb_uuid_or_string_t>) -> std_Set<ovsdb_uuid> {
    std_Set::from_iter(ids.x.iter().map(|x| ovsdb_extract_uuid(x)))
}


