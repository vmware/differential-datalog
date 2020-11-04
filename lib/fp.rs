use ddlog_bigint::*;
use num::bigint::BigInt;
use num::traits::FromPrimitive;
use ordered_float::OrderedFloat;

pub fn floor_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.floor())
}

pub fn ceil_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.ceil())
}

pub fn round_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.round())
}

pub fn trunc_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.trunc())
}

pub fn fract_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.fract())
}

pub fn abs_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.abs())
}

pub fn signum_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.signum())
}

pub fn sqrt_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.sqrt())
}

pub fn exp_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.exp())
}

pub fn exp2_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.exp2())
}

pub fn ln_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.ln())
}

pub fn log2_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.log2())
}

pub fn log10_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.log10())
}

pub fn cbrt_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.cbrt())
}

pub fn sin_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.sin())
}

pub fn cos_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.cos())
}

pub fn tan_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.tan())
}

pub fn asin_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.asin())
}

pub fn acos_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.acos())
}

pub fn atan_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.atan())
}

pub fn atan2_f(f: &OrderedFloat<f32>, other: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.atan2(**other))
}

pub fn sinh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.sinh())
}

pub fn cosh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.cosh())
}

pub fn tanh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.tanh())
}

pub fn asinh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.asinh())
}

pub fn acosh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.acosh())
}

pub fn atanh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.atanh())
}

pub fn recip_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.recip())
}

pub fn to_degrees_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.to_degrees())
}

pub fn to_radians_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.to_radians())
}

pub fn is_nan_f(f: &OrderedFloat<f32>) -> bool {
    f.is_nan()
}

pub fn is_infinite_f(f: &OrderedFloat<f32>) -> bool {
    f.is_infinite()
}

pub fn is_finite_f(f: &OrderedFloat<f32>) -> bool {
    f.is_normal()
}

pub fn mul_add_f(
    a: &OrderedFloat<f32>,
    b: &OrderedFloat<f32>,
    c: &OrderedFloat<f32>,
) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(a.mul_add(**b, **c))
}

pub fn powi_f(n: &OrderedFloat<f32>, exp: &i32) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(n.powi(*exp))
}

pub fn powf_f(n: &OrderedFloat<f32>, exp: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(n.powf(**exp))
}

pub fn log_f(n: &OrderedFloat<f32>, base: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(n.log(**base))
}

pub fn nan_f() -> OrderedFloat<f32> {
    OrderedFloat::<f32>(::std::f32::NAN)
}

//-------------------------------

pub fn floor_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.floor())
}

pub fn ceil_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.ceil())
}

pub fn round_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.round())
}

pub fn trunc_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.trunc())
}

pub fn fract_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.fract())
}

pub fn abs_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.abs())
}

pub fn signum_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.signum())
}

pub fn sqrt_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.sqrt())
}

pub fn exp_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.exp())
}

pub fn exp2_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.exp2())
}

pub fn ln_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.ln())
}

pub fn log2_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.log2())
}

pub fn log10_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.log10())
}

pub fn cbrt_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.cbrt())
}

pub fn sin_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.sin())
}

pub fn cos_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.cos())
}

pub fn tan_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.tan())
}

pub fn asin_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.asin())
}

pub fn acos_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.acos())
}

pub fn atan_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.atan())
}

pub fn atan2_d(f: &OrderedFloat<f64>, other: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.atan2(**other))
}

pub fn sinh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.sinh())
}

pub fn cosh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.cosh())
}

pub fn tanh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.tanh())
}

pub fn asinh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.asinh())
}

pub fn acosh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.acosh())
}

pub fn atanh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.atanh())
}

pub fn recip_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.recip())
}

pub fn to_degrees_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.to_degrees())
}

pub fn to_radians_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.to_radians())
}

pub fn is_nan_d(f: &OrderedFloat<f64>) -> bool {
    f.is_nan()
}

pub fn is_infinite_d(f: &OrderedFloat<f64>) -> bool {
    f.is_infinite()
}

pub fn is_finite_d(f: &OrderedFloat<f64>) -> bool {
    f.is_normal()
}

pub fn mul_add_d(
    a: &OrderedFloat<f64>,
    b: &OrderedFloat<f64>,
    c: &OrderedFloat<f64>,
) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(a.mul_add(**b, **c))
}

pub fn powi_d(n: &OrderedFloat<f64>, exp: &i32) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(n.powi(*exp))
}

pub fn powf_d(n: &OrderedFloat<f64>, exp: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(n.powf(**exp))
}

pub fn log_d(n: &OrderedFloat<f64>, base: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(n.log(**base))
}

pub fn nan_d() -> OrderedFloat<f64> {
    OrderedFloat::<f64>(::std::f64::NAN)
}

pub fn int_from_f(v: &OrderedFloat<f32>) -> ddlog_std::Option<Int> {
    match (BigInt::from_f32(**v)) {
        None => ddlog_std::Option::None,
        Some(x) => ddlog_std::Option::Some {
            x: Int::from_bigint(x),
        },
    }
}

pub fn int_from_d(v: &OrderedFloat<f64>) -> ddlog_std::Option<Int> {
    match (BigInt::from_f64(**v)) {
        None => ddlog_std::Option::None,
        Some(x) => ddlog_std::Option::Some {
            x: Int::from_bigint(x),
        },
    }
}

pub fn parse_f(s: &String) -> ddlog_std::Result<OrderedFloat<f32>, String> {
    match (s.parse::<f32>()) {
        Ok(res) => ddlog_std::Result::Ok {
            res: OrderedFloat::<f32>(res),
        },
        Err(err) => ddlog_std::Result::Err {
            err: format!("{}", err),
        },
    }
}

pub fn parse_d(s: &String) -> ddlog_std::Result<OrderedFloat<f64>, String> {
    match (s.parse::<f64>()) {
        Ok(res) => ddlog_std::Result::Ok {
            res: OrderedFloat::<f64>(res),
        },
        Err(err) => ddlog_std::Result::Err {
            err: format!("{}", err),
        },
    }
}
