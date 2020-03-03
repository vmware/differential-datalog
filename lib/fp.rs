pub fn fp_floor_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.floor())
}

pub fn fp_ceil_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.ceil())
}

pub fn fp_round_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.round())
}

pub fn fp_trunc_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.trunc())
}

pub fn fp_fract_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.fract())
}

pub fn fp_abs_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.abs())
}

pub fn fp_signum_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.signum())
}

pub fn fp_sqrt_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.sqrt())
}

pub fn fp_exp_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.exp())
}

pub fn fp_exp2_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.exp2())
}

pub fn fp_ln_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.ln())
}

pub fn fp_log2_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.log2())
}

pub fn fp_log10_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.log10())
}

pub fn fp_cbrt_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.cbrt())
}

pub fn fp_sin_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.sin())
}

pub fn fp_cos_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.cos())
}

pub fn fp_tan_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.tan())
}

pub fn fp_asin_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.asin())
}

pub fn fp_acos_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.acos())
}

pub fn fp_atan_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.atan())
}

pub fn fp_atan2_f(f: &OrderedFloat<f32>, other: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.atan2(**other))
}

pub fn fp_sinh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.sinh())
}

pub fn fp_cosh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.cosh())
}

pub fn fp_tanh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.tanh())
}

pub fn fp_asinh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.asinh())
}

pub fn fp_acosh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.acosh())
}

pub fn fp_atanh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.atanh())
}

pub fn fp_recip_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.recip())
}

pub fn fp_to_degrees_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.to_degrees())
}

pub fn fp_to_radians_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.to_radians())
}

pub fn fp_is_nan_f(f: &OrderedFloat<f32>) -> bool {
    f.is_nan()
}

pub fn fp_is_infinite_f(f: &OrderedFloat<f32>) -> bool {
    f.is_infinite()
}

pub fn fp_is_finite_f(f: &OrderedFloat<f32>) -> bool {
    f.is_normal()
}

pub fn fp_mul_add_f(
    a: &OrderedFloat<f32>,
    b: &OrderedFloat<f32>,
    c: &OrderedFloat<f32>,
) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(a.mul_add(**b, **c))
}

pub fn fp_powi_f(n: &OrderedFloat<f32>, exp: &i32) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(n.powi(*exp))
}

pub fn fp_powf_f(n: &OrderedFloat<f32>, exp: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(n.powf(**exp))
}

pub fn fp_log_f(n: &OrderedFloat<f32>, base: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(n.log(**base))
}

pub fn fp_nan_f() -> OrderedFloat<f32> {
    OrderedFloat::<f32>(std::f32::NAN)
}

//-------------------------------

pub fn fp_floor_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.floor())
}

pub fn fp_ceil_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.ceil())
}

pub fn fp_round_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.round())
}

pub fn fp_trunc_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.trunc())
}

pub fn fp_fract_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.fract())
}

pub fn fp_abs_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.abs())
}

pub fn fp_signum_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.signum())
}

pub fn fp_sqrt_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.sqrt())
}

pub fn fp_exp_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.exp())
}

pub fn fp_exp2_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.exp2())
}

pub fn fp_ln_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.ln())
}

pub fn fp_log2_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.log2())
}

pub fn fp_log10_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.log10())
}

pub fn fp_cbrt_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.cbrt())
}

pub fn fp_sin_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.sin())
}

pub fn fp_cos_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.cos())
}

pub fn fp_tan_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.tan())
}

pub fn fp_asin_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.asin())
}

pub fn fp_acos_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.acos())
}

pub fn fp_atan_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.atan())
}

pub fn fp_atan2_d(f: &OrderedFloat<f64>, other: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.atan2(**other))
}

pub fn fp_sinh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.sinh())
}

pub fn fp_cosh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.cosh())
}

pub fn fp_tanh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.tanh())
}

pub fn fp_asinh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.asinh())
}

pub fn fp_acosh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.acosh())
}

pub fn fp_atanh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.atanh())
}

pub fn fp_recip_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.recip())
}

pub fn fp_to_degrees_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.to_degrees())
}

pub fn fp_to_radians_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.to_radians())
}

pub fn fp_is_nan_d(f: &OrderedFloat<f64>) -> bool {
    f.is_nan()
}

pub fn fp_is_infinite_d(f: &OrderedFloat<f64>) -> bool {
    f.is_infinite()
}

pub fn fp_is_finite_d(f: &OrderedFloat<f64>) -> bool {
    f.is_normal()
}

pub fn fp_mul_add_d(
    a: &OrderedFloat<f64>,
    b: &OrderedFloat<f64>,
    c: &OrderedFloat<f64>,
) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(a.mul_add(**b, **c))
}

pub fn fp_powi_d(n: &OrderedFloat<f64>, exp: &i32) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(n.powi(*exp))
}

pub fn fp_powf_d(n: &OrderedFloat<f64>, exp: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(n.powf(**exp))
}

pub fn fp_log_d(n: &OrderedFloat<f64>, base: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(n.log(**base))
}

pub fn fp_nan_d() -> OrderedFloat<f64> {
    OrderedFloat::<f64>(std::f64::NAN)
}

pub fn fp_int_from_f(v: &OrderedFloat<f32>) -> std_Option<Int> {
    match (BigInt::from_f32(**v)) {
        None => std_Option::std_None,
        Some(x) => std_Option::std_Some{
            x: Int::from_bigint(x)
        },
    }
}

pub fn fp_int_from_d(v: &OrderedFloat<f64>) -> std_Option<Int> {
    match (BigInt::from_f64(**v)) {
        None => std_Option::std_None,
        Some(x) => std_Option::std_Some{
            x: Int::from_bigint(x)
        },
    }
}
