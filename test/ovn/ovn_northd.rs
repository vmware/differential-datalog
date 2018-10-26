use std::iter::FromIterator;
use ddlog_ovsdb_adapter::types_uuid2str;

fn types_map_extract_val_uuids<K: Val>(ids: &std_Map<K, types_uuid_or_string_t>) -> std_Map<K, types_uuid> {
    std_Map::from_iter(ids.x.iter().map(|(k,v)| (k.clone(), types_extract_uuid(v))))
}
fn types_set_extract_uuids(ids: &std_Set<types_uuid_or_string_t>) -> std_Set<types_uuid> {
    std_Set::from_iter(ids.x.iter().map(|x| types_extract_uuid(x)))
}


