pub fn parameterized<A: ddlog_rt::Val>(x: &A, y: &A) -> A {
    x.clone()
}

pub fn f() -> ddlog_bigint::Int {
    ddlog_bigint::Int::default()
}

pub fn g(a: &ddlog_bigint::Int) -> ddlog_bigint::Int {
    ddlog_bigint::Int::default()
}

pub fn h(a: &ddlog_std::tuple2<ddlog_bigint::Int, ddlog_bigint::Int>) -> ddlog_std::tuple2<ddlog_bigint::Int, ddlog_bigint::Int> {
    ddlog_std::tuple2::default()
}
