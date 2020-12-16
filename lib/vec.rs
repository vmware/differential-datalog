use ddlog_rt::Closure;

pub fn vec_sort_by<A, B: Ord>(v: &mut ddlog_std::Vec<A>, f: &Box<dyn Closure<*const A, B>>) {
    v.sort_unstable_by_key(|x| f.call(x))
}

pub fn vec_arg_min<A: Clone, B: Ord>(
    v: &ddlog_std::Vec<A>,
    f: &Box<dyn Closure<*const A, B>>,
) -> ddlog_std::Option<A> {
    ddlog_std::Option::from(v.iter().min_by_key(|x| f.call(*x)).map(|x| x.clone()))
}

pub fn vec_arg_max<A: Clone, B: Ord>(
    v: &ddlog_std::Vec<A>,
    f: &Box<dyn Closure<*const A, B>>,
) -> ddlog_std::Option<A> {
    ddlog_std::Option::from(v.iter().max_by_key(|x| f.call(*x)).map(|x| x.clone()))
}
