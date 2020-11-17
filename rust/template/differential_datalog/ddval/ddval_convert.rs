use crate::{
    ddval::{DDVal, DDValue},
    record::IntoRecord,
};
use ordered_float::OrderedFloat;

/// Trait to convert `DDVal` into concrete value type and back.
pub trait DDValConvert: Sized {
    /// Extract reference to concrete type from `&DDVal`.  This causes undefined behavior
    /// if `v` does not contain a value of type `Self`.
    unsafe fn from_ddval_ref(v: &DDVal) -> &Self;

    unsafe fn from_ddvalue_ref(v: &DDValue) -> &Self {
        Self::from_ddval_ref(&v.val)
    }

    /// Extracts concrete value contained in `v`.  Panics if `v` does not contain a
    /// value of type `Self`.
    unsafe fn from_ddval(v: DDVal) -> Self;

    unsafe fn from_ddvalue(v: DDValue) -> Self {
        Self::from_ddval(v.into_ddval())
    }

    /// Convert a value to a `DDVal`, erasing its original type.  This is a safe conversion
    /// that cannot fail.
    fn into_ddval(self) -> DDVal;

    fn ddvalue(&self) -> DDValue;

    fn into_ddvalue(self) -> DDValue;
}

/// Macro to implement `DDValConvert` for type `t` that satisfies the following type bounds:
///
/// t: Eq + Ord + Clone + Send + Debug + Sync + Hash + PartialOrd + IntoRecord + 'static,
/// Record: Mutator<t>
///
#[macro_export]
macro_rules! decl_ddval_convert {
    ( $t:ty ) => {
        impl $crate::ddval::DDValConvert for $t {
            unsafe fn from_ddval_ref(v: &$crate::ddval::DDVal) -> &Self {
                if ::std::mem::size_of::<Self>() <= ::std::mem::size_of::<usize>() {
                    &*(&v.v as *const usize as *const Self)
                } else {
                    &*(v.v as *const Self)
                }
            }

            unsafe fn from_ddval(v: $crate::ddval::DDVal) -> Self {
                if ::std::mem::size_of::<Self>() <= ::std::mem::size_of::<usize>() {
                    let res: Self =
                        ::std::mem::transmute::<[u8; ::std::mem::size_of::<Self>()], Self>(
                            *(&v.v as *const usize as *const [u8; ::std::mem::size_of::<Self>()]),
                        );
                    ::std::mem::forget(v);
                    res
                } else {
                    let arc = ::std::sync::Arc::from_raw(v.v as *const Self);
                    ::std::sync::Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone())
                }
            }

            fn into_ddval(self) -> $crate::ddval::DDVal {
                if ::std::mem::size_of::<Self>() <= ::std::mem::size_of::<usize>() {
                    let mut v: usize = 0;
                    unsafe {
                        *(&mut v as *mut usize as *mut [u8; ::std::mem::size_of::<Self>()]) =
                            ::std::mem::transmute::<Self, [u8; ::std::mem::size_of::<Self>()]>(
                                self,
                            );
                    };
                    $crate::ddval::DDVal { v }
                } else {
                    $crate::ddval::DDVal {
                        v: ::std::sync::Arc::into_raw(::std::sync::Arc::new(self)) as usize,
                    }
                }
            }

            fn ddvalue(&self) -> $crate::ddval::DDValue {
                $crate::ddval::DDValConvert::into_ddvalue(self.clone())
            }

            fn into_ddvalue(self) -> $crate::ddval::DDValue {
                const VTABLE: $crate::ddval::DDValMethods = $crate::ddval::DDValMethods {
                    clone: {
                        fn __f(this: &$crate::ddval::DDVal) -> $crate::ddval::DDVal {
                            if ::std::mem::size_of::<$t>() <= ::std::mem::size_of::<usize>() {
                                unsafe { <$t>::from_ddval_ref(this) }.clone().into_ddval()
                            } else {
                                let arc =
                                    unsafe { ::std::sync::Arc::from_raw(this.v as *const $t) };
                                let res = $crate::ddval::DDVal {
                                    v: ::std::sync::Arc::into_raw(::std::sync::Arc::clone(&arc))
                                        as usize,
                                };
                                ::std::sync::Arc::into_raw(arc);
                                res
                            }
                        };
                        __f
                    },
                    into_record: {
                        fn __f(this: $crate::ddval::DDVal) -> $crate::record::Record {
                            unsafe { <$t>::from_ddval(this) }.into_record()
                        };
                        __f
                    },
                    eq: {
                        fn __f(this: &$crate::ddval::DDVal, other: &$crate::ddval::DDVal) -> bool {
                            unsafe { <$t>::from_ddval_ref(this).eq(<$t>::from_ddval_ref(other)) }
                        };
                        __f
                    },
                    partial_cmp: {
                        fn __f(
                            this: &$crate::ddval::DDVal,
                            other: &$crate::ddval::DDVal,
                        ) -> Option<::std::cmp::Ordering> {
                            unsafe {
                                <$t>::from_ddval_ref(this).partial_cmp(<$t>::from_ddval_ref(other))
                            }
                        };
                        __f
                    },
                    cmp: {
                        fn __f(
                            this: &$crate::ddval::DDVal,
                            other: &$crate::ddval::DDVal,
                        ) -> ::std::cmp::Ordering {
                            unsafe { <$t>::from_ddval_ref(this).cmp(<$t>::from_ddval_ref(other)) }
                        };
                        __f
                    },
                    hash: {
                        fn __f(
                            this: &$crate::ddval::DDVal,
                            mut state: &mut dyn ::std::hash::Hasher,
                        ) {
                            ::std::hash::Hash::hash(
                                unsafe { <$t>::from_ddval_ref(this) },
                                &mut state,
                            );
                        };
                        __f
                    },
                    mutate: {
                        fn __f(
                            this: &mut $crate::ddval::DDVal,
                            record: &$crate::record::Record,
                        ) -> Result<(), ::std::string::String> {
                            let mut clone = unsafe { <$t>::from_ddval_ref(this) }.clone();
                            $crate::record::Mutator::mutate(record, &mut clone)?;
                            *this = clone.into_ddval();
                            Ok(())
                        };
                        __f
                    },
                    fmt_debug: {
                        fn __f(
                            this: &$crate::ddval::DDVal,
                            f: &mut ::std::fmt::Formatter,
                        ) -> Result<(), ::std::fmt::Error> {
                            ::std::fmt::Debug::fmt(unsafe { <$t>::from_ddval_ref(this) }, f)
                        };
                        __f
                    },
                    fmt_display: {
                        fn __f(
                            this: &$crate::ddval::DDVal,
                            f: &mut ::std::fmt::Formatter,
                        ) -> Result<(), ::std::fmt::Error> {
                            ::std::fmt::Display::fmt(
                                &unsafe { <$t>::from_ddval_ref(this) }.clone().into_record(),
                                f,
                            )
                        };
                        __f
                    },
                    drop: {
                        fn __f(this: &mut $crate::ddval::DDVal) {
                            if ::std::mem::size_of::<$t>() <= ::std::mem::size_of::<usize>() {
                                unsafe {
                                    let _v: $t = ::std::mem::transmute::<
                                        [u8; ::std::mem::size_of::<$t>()],
                                        $t,
                                    >(
                                        *(&this.v as *const usize
                                            as *const [u8; ::std::mem::size_of::<$t>()]),
                                    );
                                };
                            // v's destructor will do the rest.
                            } else {
                                let _arc =
                                    unsafe { ::std::sync::Arc::from_raw(this.v as *const $t) };
                                // arc's destructor will do the rest.
                            }
                        };
                        __f
                    },
                    ddval_serialize: {
                        fn __f(this: &$crate::ddval::DDVal) -> &dyn erased_serde::Serialize {
                            (unsafe { <$t>::from_ddval_ref(this) }) as &dyn erased_serde::Serialize
                        };
                        __f
                    },
                };
                $crate::ddval::DDValue::new(self.into_ddval(), &VTABLE)
            }
        }
    };
}

/* Implement `DDValConvert` for builtin types. */

decl_ddval_convert! {()}
decl_ddval_convert! {u8}
decl_ddval_convert! {u16}
decl_ddval_convert! {u32}
decl_ddval_convert! {u64}
decl_ddval_convert! {u128}
decl_ddval_convert! {i8}
decl_ddval_convert! {i16}
decl_ddval_convert! {i32}
decl_ddval_convert! {i64}
decl_ddval_convert! {i128}
decl_ddval_convert! {String}
decl_ddval_convert! {bool}
decl_ddval_convert! {OrderedFloat<f32>}
decl_ddval_convert! {OrderedFloat<f64>}
