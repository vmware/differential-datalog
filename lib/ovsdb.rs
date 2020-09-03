pub use ddlog_ovsdb_adapter::uuid2name;
pub use ddlog_ovsdb_adapter::uuid2str;
use std::iter::FromIterator;

use crate::ddlog_std;
use crate::Val;

pub fn map_extract_val_uuids<K: Val>(
    ids: &ddlog_std::Map<K, uuid_or_string_t>,
) -> ddlog_std::Map<K, uuid> {
    ddlog_std::Map::from_iter(ids.x.iter().map(|(k, v)| (k.clone(), extract_uuid(v))))
}
pub fn set_extract_uuids(ids: &ddlog_std::Set<uuid_or_string_t>) -> ddlog_std::Set<uuid> {
    ddlog_std::Set::from_iter(ids.x.iter().map(|x| extract_uuid(x)))
}

pub fn group2vec_remove_sentinel<K>(
    g: &ddlog_std::Group<K, uuid_or_string_t>,
) -> ddlog_std::Vec<uuid_or_string_t> {
    let mut res = ddlog_std::Vec::new();
    for ref v in g.iter() {
        match v {
            ddlog_std::Either::Right { r } => {
                if r.as_str() != "" {
                    res.push(ddlog_std::Either::Right { r: r.clone() });
                };
            }
            v => {
                res.push(v.clone());
            }
        }
    }
    res
}

pub fn group2set_remove_sentinel<K>(
    g: &ddlog_std::Group<K, uuid_or_string_t>,
) -> ddlog_std::Set<uuid_or_string_t> {
    let mut res = ddlog_std::Set::new();
    for ref v in g.iter() {
        match v {
            ddlog_std::Either::Right { r } => {
                if r.as_str() != "" {
                    res.insert(ddlog_std::Either::Right { r: r.clone() });
                };
            }
            v => {
                res.insert(v.clone());
            }
        }
    }
    res
}

pub fn group2map_remove_sentinel<K1, K2: Ord + Clone>(
    g: &ddlog_std::Group<K1, (K2, uuid_or_string_t)>,
) -> ddlog_std::Map<K2, uuid_or_string_t> {
    let mut res = ddlog_std::Map::new();
    for (ref k, ref v) in g.iter() {
        match v {
            ddlog_std::Either::Right { r } => {
                if r.as_str() != "" {
                    res.insert((*k).clone(), ddlog_std::Either::Right { r: r.clone() });
                };
            }
            _ => {
                res.insert((*k).clone(), v.clone());
            }
        }
    }
    res
}

pub fn set_map_uuid2str(ids: &ddlog_std::Set<uuid>) -> ddlog_std::Set<String> {
    ddlog_std::Set::from_iter(ids.x.iter().map(|id| uuid2str(id)))
}

pub fn set_map_uuid2name(ids: &ddlog_std::Set<uuid>) -> ddlog_std::Set<String> {
    ddlog_std::Set::from_iter(ids.x.iter().map(|id| uuid2name(id)))
}
