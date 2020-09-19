use crate::closure::Closure;
use crate::ddlog_std;

pub fn map_map_in_place<K: Ord, V>(
    m: &mut ddlog_std::Map<K, V>,
    f: &Box<dyn Closure<(*const K, *mut V), ()>>,
) {
    for (k, v) in m.x.iter_mut() {
        f.call((k, v))
    }
}
