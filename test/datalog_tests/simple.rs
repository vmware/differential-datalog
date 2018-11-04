fn parameterized<A: Val>(x: &A, y: &A) -> A {
    x.clone()
}

fn allocate_u32<B: Ord+Clone>(allocated: &std_Set<u32>, toallocate: &std_Vec<B>, max_val: &u32) -> std_Vec<(B,u32)> {
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
