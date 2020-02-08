pub use ddlog_ovsdb_adapter::ovsdb_uuid2name;
pub use ddlog_ovsdb_adapter::ovsdb_uuid2str;
use std::iter::FromIterator;

pub fn ovsdb_map_extract_val_uuids<K: Val>(
    ids: &std_Map<K, ovsdb_uuid_or_string_t>,
) -> std_Map<K, ovsdb_uuid> {
    std_Map::from_iter(
        ids.x
            .iter()
            .map(|(k, v)| (k.clone(), ovsdb_extract_uuid(v))),
    )
}
pub fn ovsdb_set_extract_uuids(ids: &std_Set<ovsdb_uuid_or_string_t>) -> std_Set<ovsdb_uuid> {
    std_Set::from_iter(ids.x.iter().map(|x| ovsdb_extract_uuid(x)))
}

pub fn ovsdb_group2vec_remove_sentinel<K>(
    g: &std_Group<K, ovsdb_uuid_or_string_t>,
) -> std_Vec<ovsdb_uuid_or_string_t> {
    let mut res = std_Vec::new();
    for ref v in g.iter() {
        match v {
            std_Either::std_Right { r } => {
                if r.as_str() != "" {
                    res.push(std_Either::std_Right { r: r.clone() });
                };
            }
            v => {
                res.push(v.clone());
            }
        }
    }
    res
}

pub fn ovsdb_group2set_remove_sentinel<K>(
    g: &std_Group<K, ovsdb_uuid_or_string_t>,
) -> std_Set<ovsdb_uuid_or_string_t> {
    let mut res = std_Set::new();
    for ref v in g.iter() {
        match v {
            std_Either::std_Right { r } => {
                if r.as_str() != "" {
                    res.insert(std_Either::std_Right { r: r.clone() });
                };
            }
            v => {
                res.insert(v.clone());
            }
        }
    }
    res
}

pub fn ovsdb_group2map_remove_sentinel<K1, K2: Ord + Clone>(
    g: &std_Group<K1, (K2, ovsdb_uuid_or_string_t)>,
) -> std_Map<K2, ovsdb_uuid_or_string_t> {
    let mut res = std_Map::new();
    for (ref k, ref v) in g.iter() {
        match v {
            std_Either::std_Right { r } => {
                if r.as_str() != "" {
                    res.insert((*k).clone(), std_Either::std_Right { r: r.clone() });
                };
            }
            _ => {
                res.insert((*k).clone(), v.clone());
            }
        }
    }
    res
}

pub fn ovsdb_set_map_uuid2str(ids: &std_Set<ovsdb_uuid>) -> std_Set<String> {
    std_Set::from_iter(ids.x.iter().map(|id| ovsdb_uuid2str(id)))
}

pub fn ovsdb_set_map_uuid2name(ids: &std_Set<ovsdb_uuid>) -> std_Set<String> {
    std_Set::from_iter(ids.x.iter().map(|id| ovsdb_uuid2name(id)))
}
