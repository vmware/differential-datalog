use std::iter::FromIterator;
pub use ddlog_ovsdb_adapter::ovsdb_uuid2str;

pub fn ovsdb_map_extract_val_uuids<K: Val>(ids: &std_Map<K, ovsdb_uuid_or_string_t>) -> std_Map<K, ovsdb_uuid> {
    std_Map::from_iter(ids.x.iter().map(|(k,v)| (k.clone(), ovsdb_extract_uuid(v))))
}
pub fn ovsdb_set_extract_uuids(ids: &std_Set<ovsdb_uuid_or_string_t>) -> std_Set<ovsdb_uuid> {
    std_Set::from_iter(ids.x.iter().map(|x| ovsdb_extract_uuid(x)))
}

pub fn ovsdb_group2vec_remove_sentinel<G: Group<ovsdb_uuid_or_string_t>+?Sized>(g: &G) -> std_Vec<ovsdb_uuid_or_string_t> {
    let mut res = std_Vec::new();
    for i in 0..g.size() {
        match g.ith(i) {
            std_Either::std_Right{r} => {
                if r.str() != "" { res.push(std_Either::std_Right{r}); };
            },
            v => { res.push(v); }
        }
    };
    res
}

pub fn ovsdb_group2set_remove_sentinel<G: Group<ovsdb_uuid_or_string_t>+?Sized>(g: &G) -> std_Set<ovsdb_uuid_or_string_t> {
    let mut res = std_Set::new();
    for i in 0..g.size() {
        match g.ith(i) {
            std_Either::std_Right{r} => {
                if r.str() != "" { res.insert(std_Either::std_Right{r}); };
            },
            v => { res.insert(v); }
        }
    };
    res
}


pub fn ovsdb_group2map_remove_sentinel<K: Ord, G:Group<(K,ovsdb_uuid_or_string_t)>+?Sized>(g: &G) -> std_Map<K,ovsdb_uuid_or_string_t> {
    let mut res = std_Map::new();
    for i in 0..g.size() {
        let (k,v) = g.ith(i);
        match v {
            std_Either::std_Right{r} => {
                if r.str() != "" { res.insert(k, std_Either::std_Right{r}); };
            },
            _ => { res.insert(k, v); }
        }
    };
    res
}

pub fn ovsdb_set_map_uuid2str(ids: &std_Set<ovsdb_uuid>) -> std_Set<arcval::DDString> {
    std_Set::from_iter(ids.x.iter().map(|id| ovsdb_uuid2str(id)))
}
