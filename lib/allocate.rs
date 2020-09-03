use std::cmp;
use std::collections::BTreeSet;
use std::ops;

pub fn allocate<B: Ord + Clone, N: num::Num + ops::Add + cmp::Ord + Copy>(
    allocated: &crate::ddlog_std::Set<N>,
    toallocate: &crate::ddlog_std::Vec<B>,
    min_val: &N,
    max_val: &N,
) -> crate::ddlog_std::Vec<(B, N)> {
    assert!(*max_val >= *min_val);
    let range = *max_val - *min_val;

    // Next index to consider
    let mut next = *allocated.x.range(..).next_back().unwrap_or(&max_val);
    // allocated may contain values outside of the [min_val..max_val] range.
    if next < *min_val || next > *max_val {
        next = *max_val;
    };
    let mut offset = N::zero();
    let mut res = crate::ddlog_std::Vec::new();
    for b in toallocate.x.iter() {
        loop {
            next = if next == *max_val {
                *min_val
            } else {
                next + N::one()
            };

            if allocated.x.contains(&next) {
                if offset == range {
                    return res;
                };
                offset = offset + N::one();
                continue;
            } else {
                res.x.push(((*b).clone(), next));
                if offset == range {
                    return res;
                };
                offset = offset + N::one();
                break;
            }
        }
    }
    return res;
}

pub fn allocate_with_hint<B: Ord + Clone, N: num::Num + ops::Add + cmp::Ord + Copy>(
    allocated: &crate::ddlog_std::Set<N>,
    toallocate: &crate::ddlog_std::Vec<(B, crate::ddlog_std::Option<N>)>,
    min_val: &N,
    max_val: &N,
) -> crate::ddlog_std::Vec<(B, N)> {
    assert!(*max_val >= *min_val);
    let range = *max_val - *min_val;
    let mut new_allocations: BTreeSet<N> = BTreeSet::new();

    // Next index to consider
    let mut next = *allocated.x.range(..).next_back().unwrap_or(&min_val);
    // allocated may contain values outside of the [min_val..max_val] range.
    if next < *min_val || next > *max_val {
        next = *min_val;
    };
    let mut res = crate::ddlog_std::Vec::new();
    for (b, hint) in toallocate.x.iter() {
        let mut offset = N::zero();
        next = match hint {
            crate::ddlog_std::Option::None => next,
            crate::ddlog_std::Option::Some { x: h } => *h,
        };
        loop {
            if allocated.x.contains(&next) || new_allocations.contains(&next) {
                if offset == range {
                    return res;
                };
                offset = offset + N::one();
                next = if next == *max_val {
                    *min_val
                } else {
                    next + N::one()
                };
                continue;
            } else {
                res.x.push(((*b).clone(), next));
                new_allocations.insert(next);
                if offset == range {
                    return res;
                };
                next = if next == *max_val {
                    *min_val
                } else {
                    next + N::one()
                };
                break;
            }
        }
    }
    return res;
}

pub fn allocate_opt<B: Ord + Clone, N: num::Num + ops::Add + cmp::Ord + Copy>(
    allocated: &crate::ddlog_std::Set<N>,
    toallocate: &crate::ddlog_std::Vec<B>,
    min_val: &N,
    max_val: &N,
) -> crate::ddlog_std::Vec<(B, crate::ddlog_std::Option<N>)> {
    assert!(*max_val >= *min_val);
    let range = *max_val - *min_val;

    // Next index to consider
    let mut next = *allocated.x.range(..).next_back().unwrap_or(&max_val);
    // allocated may contain values outside of the [min_val..max_val] range.
    if next < *min_val || next > *max_val {
        next = *max_val;
    };
    let mut offset = N::zero();
    let mut res = crate::ddlog_std::Vec::new();
    // Signal that address space has been exhausted, but iteration must continue to
    // assign `None` to all remaining items.
    let mut exhausted = false;
    for b in toallocate.x.iter() {
        loop {
            if exhausted {
                res.x.push(((*b).clone(), crate::ddlog_std::Option::None));
                break;
            };
            if offset == range {
                exhausted = true;
            };
            next = if next == *max_val {
                *min_val
            } else {
                next + N::one()
            };
            offset = offset + N::one();

            if allocated.x.contains(&next) {
                continue;
            } else {
                res.x
                    .push(((*b).clone(), crate::ddlog_std::Option::Some { x: next }));
                break;
            }
        }
    }
    return res;
}

pub fn adjust_allocation<A: Ord + Clone, N: num::Num + ops::Add + cmp::Ord + Copy>(
    allocated: &crate::ddlog_std::Map<A, N>,
    toallocate: &crate::ddlog_std::Vec<A>,
    min_val: &N,
    max_val: &N,
) -> crate::ddlog_std::Vec<(A, N)> {
    assert!(*max_val >= *min_val);
    let range = *max_val - *min_val;

    let allocated_ids: BTreeSet<N> = allocated.x.values().cloned().collect();
    let mut next = *allocated_ids.range(..).next_back().unwrap_or(&max_val);
    // allocated may contain values outside of the [min_val..max_val] range.
    if next < *min_val || next > *max_val {
        next = *max_val;
    };
    let mut offset = N::zero();
    let mut res = crate::ddlog_std::Vec::new();
    // Signal that address space has been exhausted, but iteration must continue to
    // preserve existing allocations.
    let mut exhausted = false;
    for b in toallocate.x.iter() {
        match allocated.x.get(b) {
            Some(x) => {
                res.push(((*b).clone(), x.clone()));
            }
            None => loop {
                if exhausted {
                    break;
                };
                if offset == range {
                    exhausted = true;
                };
                next = if next == *max_val {
                    *min_val
                } else {
                    next + N::one()
                };
                offset = offset + N::one();

                if allocated_ids.contains(&next) {
                    continue;
                } else {
                    res.x.push(((*b).clone(), next));
                    break;
                }
            },
        }
    }
    return res;
}
