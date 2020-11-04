use crate::ddlog_rt::Closure;
use crate::ddlog_std;

pub fn set_arg_min<A: Ord + Clone, B: Ord>(
    s: &ddlog_std::Set<A>,
    f: &Box<dyn Closure<*const A, B>>,
) -> ddlog_std::Option<A> {
    ddlog_std::Option::from(s.x.iter().min_by_key(|x| f.call(*x)).map(|x| x.clone()))
}

pub fn set_arg_max<A: Ord + Clone, B: Ord>(
    s: &ddlog_std::Set<A>,
    f: &Box<dyn Closure<*const A, B>>,
) -> ddlog_std::Option<A> {
    ddlog_std::Option::from(s.x.iter().max_by_key(|x| f.call(*x)).map(|x| x.clone()))
}
