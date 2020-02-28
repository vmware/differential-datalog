pub fn libfloat_floor_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.floor())
}

pub fn libfloat_ceil_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.ceil())
}

pub fn libfloat_round_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.round())
}

pub fn libfloat_trunc_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.trunc())
}

pub fn libfloat_fract_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.fract())
}

pub fn libfloat_abs_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.abs())
}

pub fn libfloat_signum_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.signum())
}

pub fn libfloat_sqrt_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.sqrt())
}

pub fn libfloat_exp_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.exp())
}

pub fn libfloat_exp2_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.exp2())
}

pub fn libfloat_ln_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.ln())
}

pub fn libfloat_log2_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.log2())
}

pub fn libfloat_log10_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.log10())
}

pub fn libfloat_cbrt_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.cbrt())
}

pub fn libfloat_sin_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.sin())
}

pub fn libfloat_cos_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.cos())
}

pub fn libfloat_tan_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.tan())
}

pub fn libfloat_asin_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.asin())
}

pub fn libfloat_acos_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.acos())
}

pub fn libfloat_atan_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.atan())
}

pub fn libfloat_atan2_f(f: &OrderedFloat<f32>, other: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.atan2(**other))
}

pub fn libfloat_sinh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.sinh())
}

pub fn libfloat_cosh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.cosh())
}

pub fn libfloat_tanh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.tanh())
}

pub fn libfloat_asinh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.asinh())
}

pub fn libfloat_acosh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.acosh())
}

pub fn libfloat_atanh_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.atanh())
}

pub fn libfloat_recip_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.recip())
}

pub fn libfloat_to_degrees_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.to_degrees())
}

pub fn libfloat_to_radians_f(f: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(f.to_radians())
}

pub fn libfloat_is_nan_f(f: &OrderedFloat<f32>) -> bool {
    f.is_nan()
}

pub fn libfloat_is_infinite_f(f: &OrderedFloat<f32>) -> bool {
    f.is_infinite()
}

pub fn libfloat_is_finite_f(f: &OrderedFloat<f32>) -> bool {
    f.is_normal()
}

pub fn libfloat_mul_add_f(
    a: &OrderedFloat<f32>,
    b: &OrderedFloat<f32>,
    c: &OrderedFloat<f32>
) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(a.mul_add(**b, **c))
}

pub fn libfloat_powi_f(n: &OrderedFloat<f32>, exp: i32) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(n.powi(exp))
}

pub fn libfloat_powf_f(n: &OrderedFloat<f32>, exp: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(n.powf(**exp))
}

pub fn libfloat_log_f(n: &OrderedFloat<f32>, base: &OrderedFloat<f32>) -> OrderedFloat<f32> {
    OrderedFloat::<f32>(n.log(**base))
}

pub fn libfloat_nan_f() -> OrderedFloat<f32> {
    OrderedFloat::<f32>(std::f32::NAN)
}

//-------------------------------

pub fn libfloat_floor_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.floor())
}

pub fn libfloat_ceil_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.ceil())
}

pub fn libfloat_round_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.round())
}

pub fn libfloat_trunc_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.trunc())
}

pub fn libfloat_fract_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.fract())
}

pub fn libfloat_abs_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.abs())
}

pub fn libfloat_signum_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.signum())
}

pub fn libfloat_sqrt_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.sqrt())
}

pub fn libfloat_exp_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.exp())
}

pub fn libfloat_exp2_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.exp2())
}

pub fn libfloat_ln_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.ln())
}

pub fn libfloat_log2_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.log2())
}

pub fn libfloat_log10_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.log10())
}

pub fn libfloat_cbrt_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.cbrt())
}

pub fn libfloat_sin_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.sin())
}

pub fn libfloat_cos_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.cos())
}

pub fn libfloat_tan_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.tan())
}

pub fn libfloat_asin_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.asin())
}

pub fn libfloat_acos_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.acos())
}

pub fn libfloat_atan_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.atan())
}

pub fn libfloat_atan2_d(f: &OrderedFloat<f64>, other: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.atan2(**other))
}

pub fn libfloat_sinh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.sinh())
}

pub fn libfloat_cosh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.cosh())
}

pub fn libfloat_tanh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.tanh())
}

pub fn libfloat_asinh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.asinh())
}

pub fn libfloat_acosh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.acosh())
}

pub fn libfloat_atanh_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.atanh())
}

pub fn libfloat_recip_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.recip())
}

pub fn libfloat_to_degrees_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.to_degrees())
}

pub fn libfloat_to_radians_d(f: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(f.to_radians())
}

pub fn libfloat_is_nan_d(f: &OrderedFloat<f64>) -> bool {
    f.is_nan()
}

pub fn libfloat_is_infinite_d(f: &OrderedFloat<f64>) -> bool {
    f.is_infinite()
}

pub fn libfloat_is_finite_d(f: &OrderedFloat<f64>) -> bool {
    f.is_normal()
}

pub fn libfloat_mul_add_d(
    a: &OrderedFloat<f64>,
    b: &OrderedFloat<f64>,
    c: &OrderedFloat<f64>
) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(a.mul_add(**b, **c))
}

pub fn libfloat_powi_d(n: &OrderedFloat<f64>, exp: i32) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(n.powi(exp))
}

pub fn libfloat_powf_d(n: &OrderedFloat<f64>, exp: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(n.powf(**exp))
}

pub fn libfloat_log_d(n: &OrderedFloat<f64>, base: &OrderedFloat<f64>) -> OrderedFloat<f64> {
    OrderedFloat::<f64>(n.log(**base))
}

pub fn libfloat_nan_d() -> OrderedFloat<f64> {
    OrderedFloat::<f64>(std::f64::NAN)
}
