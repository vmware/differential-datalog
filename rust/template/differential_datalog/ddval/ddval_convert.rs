use crate::{
    ddval::{DDVal, DDValue},
    record::IntoRecord,
};
use ordered_float::OrderedFloat;
use std::any::TypeId;

/// Trait to convert `DDVal` into concrete value type and back.
pub trait DDValConvert: Sized {
    /// Extract reference to concrete type from `&DDVal`.
    ///
    /// # Safety
    ///
    /// `value` **must** be the same type as the type the `DDVal` was created with
    ///
    unsafe fn from_ddval_ref(value: &DDVal) -> &Self;

    /// Converts an `&DDValue` into a reference of the given type
    ///
    /// Returns `None` if the type given is not the same as the type the `DDValue`
    /// was created with
    ///
    fn try_from_ddvalue_ref(value: &DDValue) -> Option<&Self>
    where
        Self: 'static,
    {
        let value_type = (value.vtable.type_id)(&value.val);
        if value_type == TypeId::of::<Self>() {
            // Safety: The type we're turning the value into is the same as the one
            //         it was created with
            Some(unsafe { Self::from_ddval_ref(&value.val) })
        } else {
            None
        }
    }

    /// Converts an `&DDValue` into a reference of the given type
    ///
    /// # Panics
    ///
    /// Panics if the type given is not the same as the type the `DDValue`
    /// was created with
    ///
    fn from_ddvalue_ref(value: &DDValue) -> &Self
    where
        Self: 'static,
    {
        Self::try_from_ddvalue_ref(value)
            .expect("attempted to convert a DDValue into the incorrect type")
    }

    /// Extracts concrete value contained in `value`.
    ///
    /// # Safety
    ///
    /// `value` **must** be the same type as the type the `DDValue` was created with
    ///
    unsafe fn from_ddval(value: DDVal) -> Self;

    /// Converts a `DDValue` into a the given type
    ///
    /// Returns `None` if the type given is not the same as the type the `DDValue`
    /// was created with
    ///
    fn try_from_ddvalue(value: DDValue) -> Option<Self>
    where
        Self: 'static,
    {
        let value_type = (value.vtable.type_id)(&value.val);
        if value_type == TypeId::of::<Self>() {
            // Safety: The type we're turning the value into is the same as the one
            //         it was created with
            Some(unsafe { Self::from_ddval(value.into_ddval()) })
        } else {
            None
        }
    }

    /// Converts a `DDValue` into the given type
    ///
    /// # Panics
    ///
    /// Panics if the type given is not the same as the type the `DDValue`
    /// was created with
    ///
    fn from_ddvalue(value: DDValue) -> Self
    where
        Self: 'static,
    {
        Self::try_from_ddvalue(value)
            .expect("attempted to convert a DDValue into the incorrect type")
    }

    /// Convert a value to a `DDVal`, erasing its original type.  This is a safe conversion
    /// that cannot fail.
    fn into_ddval(self) -> DDVal;

    /// Creates a `DDValue` from the current value
    fn ddvalue(&self) -> DDValue;

    /// Converts the current value into a `DDValue`
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
                use ::std::mem::size_of;

                if size_of::<Self>() <= size_of::<usize>() {
                    &*(&v.v as *const usize as *const Self)
                } else {
                    &*(v.v as *const Self)
                }
            }

            unsafe fn from_ddval(v: $crate::ddval::DDVal) -> Self {
                use ::std::{
                    mem::{self, size_of, transmute},
                    sync::Arc,
                };

                if size_of::<Self>() <= size_of::<usize>() {
                    let res: Self = transmute::<[u8; size_of::<Self>()], Self>(
                        *(&v.v as *const usize as *const [u8; size_of::<Self>()]),
                    );
                    mem::forget(v);

                    res
                } else {
                    let arc = Arc::from_raw(v.v as *const Self);
                    Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone())
                }
            }

            fn into_ddval(self) -> $crate::ddval::DDVal {
                use ::std::{
                    mem::{size_of, transmute},
                    sync::Arc,
                };
                use $crate::ddval::DDVal;

                if size_of::<Self>() <= size_of::<usize>() {
                    let mut v: usize = 0;
                    unsafe {
                        *(&mut v as *mut usize as *mut [u8; size_of::<Self>()]) =
                            transmute::<Self, [u8; size_of::<Self>()]>(self);
                    };

                    DDVal { v }
                } else {
                    DDVal {
                        v: Arc::into_raw(Arc::new(self)) as usize,
                    }
                }
            }

            fn ddvalue(&self) -> $crate::ddval::DDValue {
                $crate::ddval::DDValConvert::into_ddvalue(self.clone())
            }

            fn into_ddvalue(self) -> $crate::ddval::DDValue {
                use ::std::{
                    any::TypeId,
                    cmp::Ordering,
                    fmt::{self, Debug, Display, Formatter},
                    hash::{Hash, Hasher},
                    mem::{self, size_of, transmute, ManuallyDrop},
                    sync::Arc,
                };
                use $crate::{
                    ddval::{DDVal, DDValMethods, DDValue},
                    record::{Mutator, Record},
                };

                fn clone(this: &DDVal) -> DDVal {
                    if size_of::<$t>() <= size_of::<usize>() {
                        unsafe { <$t>::from_ddval_ref(this) }.clone().into_ddval()
                    } else {
                        let arc = unsafe { ManuallyDrop::new(Arc::from_raw(this.v as *const $t)) };
                        let res = DDVal {
                            v: Arc::into_raw(Arc::clone(&arc)) as usize,
                        };

                        res
                    }
                }

                fn into_record(this: DDVal) -> Record {
                    unsafe { <$t>::from_ddval(this) }.into_record()
                }

                fn eq(this: &DDVal, other: &DDVal) -> bool {
                    unsafe { <$t>::from_ddval_ref(this).eq(<$t>::from_ddval_ref(other)) }
                }

                fn partial_cmp(this: &DDVal, other: &DDVal) -> Option<Ordering> {
                    unsafe { <$t>::from_ddval_ref(this).partial_cmp(<$t>::from_ddval_ref(other)) }
                }

                fn cmp(this: &DDVal, other: &DDVal) -> Ordering {
                    unsafe { <$t>::from_ddval_ref(this).cmp(<$t>::from_ddval_ref(other)) }
                }

                fn hash(this: &DDVal, mut state: &mut dyn Hasher) {
                    Hash::hash(unsafe { <$t>::from_ddval_ref(this) }, &mut state);
                }

                fn mutate(this: &mut DDVal, record: &Record) -> Result<(), ::std::string::String> {
                    let mut clone = unsafe { <$t>::from_ddval_ref(this) }.clone();
                    Mutator::mutate(record, &mut clone)?;
                    *this = clone.into_ddval();

                    Ok(())
                }

                fn fmt_debug(this: &DDVal, f: &mut Formatter) -> Result<(), fmt::Error> {
                    Debug::fmt(unsafe { <$t>::from_ddval_ref(this) }, f)
                }

                fn fmt_display(this: &DDVal, f: &mut Formatter) -> Result<(), fmt::Error> {
                    Display::fmt(
                        &unsafe { <$t>::from_ddval_ref(this) }.clone().into_record(),
                        f,
                    )
                }

                fn drop(this: &mut DDVal) {
                    if size_of::<$t>() <= size_of::<usize>() {
                        // Allow `_val`'s Drop impl to run automatically
                        let _val = unsafe {
                            transmute::<[u8; size_of::<$t>()], $t>(
                                *(&this.v as *const usize as *const [u8; size_of::<$t>()]),
                            );
                        };
                    } else {
                        let arc = unsafe { Arc::from_raw(this.v as *const $t) };
                        mem::drop(arc);
                    }
                }

                fn ddval_serialize(this: &DDVal) -> &dyn erased_serde::Serialize {
                    (unsafe { <$t>::from_ddval_ref(this) }) as &dyn erased_serde::Serialize
                }

                fn type_id(_this: &DDVal) -> TypeId {
                    TypeId::of::<$t>()
                }

                static VTABLE: DDValMethods = DDValMethods {
                    clone,
                    into_record,
                    eq,
                    partial_cmp,
                    cmp,
                    hash,
                    mutate,
                    fmt_debug,
                    fmt_display,
                    drop,
                    ddval_serialize,
                    type_id,
                };

                DDValue::new(self.into_ddval(), &VTABLE)
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
