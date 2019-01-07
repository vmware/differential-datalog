extern crate num;
use std::collections::BTreeSet;
use std::ops;
use std::cmp;

pub fn allocate_allocate<B: Ord+Clone, N:num::Num+ops::Add+cmp::Ord+Copy>(
    allocated: &std_Set<N>,
    toallocate: &std_Vec<B>,
    min_val: &N,
    max_val: &N) -> std_Vec<(B,N)>
{
    assert!(*max_val >= *min_val);
    let range = *max_val - *min_val;

    // Next index to consider
    let mut next = *allocated.x.range(..).next_back().unwrap_or(&max_val);
    let mut offset = N::zero();
    let mut res = std_Vec::new();
    for b in toallocate.x.iter() {
        loop {
            next = if next == *max_val { *min_val } else { next + N::one() };
            offset = offset + N::one();

            if allocated.x.contains(&next) {
                if offset == range + N::one() { return res; };
                continue;
            } else {
                res.x.push(((*b).clone(), next));
                if offset == range + N::one() { return res; };
                break;
            }
        }
    };
    return res
}

pub fn allocate_adjust_allocation<A: Ord+Clone, N: num::Num+ops::Add+cmp::Ord+Copy>(
    allocated: &std_Map<A, N>,
    toallocate: &std_Vec<A>,
    min_val: &N,
    max_val: &N) -> std_Vec<(A,N)>
{
    assert!(*max_val >= *min_val);
    let range = *max_val - *min_val;

    let allocated_ids: BTreeSet<N> = allocated.x.values().cloned().collect();
    let mut next = *allocated_ids.range(..).next_back().unwrap_or(&max_val);
    let mut offset = N::zero();
    let mut res = std_Vec::new();
    for b in toallocate.x.iter() {
        match allocated.x.get(b) {
            Some(x) => { res.push(((*b).clone(), x.clone())); },
            None => loop {
                next = if next == *max_val { *min_val } else { next + N::one() };
                offset = offset + N::one();

                if allocated_ids.contains(&next) {
                    if offset == range + N::one() { return res; };
                    continue;
                } else {
                    res.x.push(((*b).clone(), next));
                    if offset == range + N::one() { return res; };
                    break;
                }
            }
        }
    };
    return res
}
