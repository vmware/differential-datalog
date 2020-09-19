use crate::closure::Closure;
use crate::ddlog_std;

pub fn vec_sort_by<A, B: Ord>(v: &mut ddlog_std::Vec<A>, f: &Box<dyn Closure<*const A, B>>) {
    v.x.sort_unstable_by_key(|x| f.call(x))
}

pub fn vec_min_by<A: Clone, B: Ord>(
    v: &ddlog_std::Vec<A>,
    f: &Box<dyn Closure<*const A, B>>,
) -> ddlog_std::Option<A> {
    ddlog_std::Option::from(v.x.iter().min_by_key(|x| f.call(*x)).map(|x| x.clone()))
}

pub fn vec_max_by<A: Clone, B: Ord>(
    v: &ddlog_std::Vec<A>,
    f: &Box<dyn Closure<*const A, B>>,
) -> ddlog_std::Option<A> {
    ddlog_std::Option::from(v.x.iter().max_by_key(|x| f.call(*x)).map(|x| x.clone()))
}
