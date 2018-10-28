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
