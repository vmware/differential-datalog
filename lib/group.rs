use std::{default::Default, ops::Add};

use ddlog_rt::Closure;
use ddlog_std::Group;

pub fn sum_of<K, V: Clone, N: Add<Output = N> + Default>(
    g: &Group<K, V>,
    f: &Box<dyn Closure<*const V, N>>,
) -> N {
    let mut sum = N::default();

    for v in g.val_iter() {
        sum = sum + f.call(v);
    }

    sum
}
