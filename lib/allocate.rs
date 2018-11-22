use std::collections::BTreeSet;

pub fn allocate_allocate_u64<B: Ord+Clone>(allocated: &std_Set<u64>, toallocate: &std_Vec<B>, max_val: &u64) -> std_Vec<(B,u64)> {
    let first = *allocated.x.range(..).next_back().unwrap_or(&0);
    let mut last = first;
    let mut res = std_Vec::new();
    for b in toallocate.x.iter() {
        loop {
            last = if last + 1 <= *max_val {last + 1} else {1};
            if last == first {
                return res;
            };
            if allocated.x.contains(&last) { continue; };
            res.x.push(((*b).clone(), last));
            break;
        }
    };
    return res
}

pub fn allocate_adjust_allocation_u64<A: Ord+Clone>(allocated: &std_Map<A, u64>, toallocate: &std_Vec<A>, max_val: &u64) -> std_Vec<(A,u64)> {
    let allocated_ids: BTreeSet<u64> = allocated.x.values().cloned().collect();
    let first = *allocated_ids.range(..).next_back().unwrap_or(&0);
    let mut last = first;
    let mut res = std_Vec::new();
    for b in toallocate.x.iter() {
        match allocated.x.get(b) {
            Some(x) => { res.push(((*b).clone(), x.clone())); },
            None => loop {
                last = if last + 1 <= *max_val {last + 1} else {1};
                if last == first {
                    return res;
                };
                if allocated_ids.contains(&last) { continue; };
                res.x.push(((*b).clone(), last));
                break;
            }
        }
    };
    return res
}
