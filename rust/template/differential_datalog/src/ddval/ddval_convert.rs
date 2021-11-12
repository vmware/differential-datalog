use crate::{
    ddval::{DDVal, DDValMethods, DDValue},
    record::{IntoRecord, Mutator, Record},
};
use std::{
    any::{Any, TypeId},
    cmp::Ordering,
    fmt::{self, Debug, Display, Formatter},
    hash::{Hash, Hasher},
    hint::unreachable_unchecked,
    mem::{self, align_of, needs_drop, size_of, ManuallyDrop},
};
use triomphe::Arc;

/// Trait to convert `DDVal` into concrete value type and back.
#[allow(clippy::upper_case_acronyms)]
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
    #[inline]
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
    #[inline]
    fn from_ddvalue_ref(value: &DDValue) -> &Self
    where
        Self: 'static,
    {
        Self::try_from_ddvalue_ref(value)
            .expect("attempted to convert a DDValue into the incorrect type")
    }

    /// Converts a [`DDValue`] into a reference of the given type without
    /// checking that the type given and the [`DDValue`]'s internal types
    /// are the same
    ///
    /// # Safety
    ///
    /// The type given must be the same as the [`DDValue`]'s internal type
    ///
    #[inline]
    unsafe fn from_ddvalue_ref_unchecked(value: &DDValue) -> &Self
    where
        Self: 'static,
    {
        let value_type = (value.vtable.type_id)(&value.val);
        debug_assert_eq!(value_type, TypeId::of::<Self>());

        Self::try_from_ddvalue_ref(value).unwrap_or_else(|| unreachable_unchecked())
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
    #[inline]
    fn try_from_ddvalue(value: DDValue) -> Result<Self, DDValue>
    where
        Self: 'static,
    {
        let value_type = (value.vtable.type_id)(&value.val);
        if value_type == TypeId::of::<Self>() {
            // Safety: The type we're turning the value into is the same as the one
            //         it was created with
            Ok(unsafe { Self::from_ddval(value.into_ddval()) })
        } else {
            Err(value)
        }
    }

    /// Converts a `DDValue` into the given type
    ///
    /// # Panics
    ///
    /// Panics if the type given is not the same as the type the `DDValue`
    /// was created with
    ///
    #[inline]
    fn from_ddvalue(value: DDValue) -> Self
    where
        Self: 'static,
    {
        Self::try_from_ddvalue(value)
            .expect("attempted to convert a DDValue into the incorrect type")
    }

    /// Converts a [`DDValue`] into the given type without checking that the
    /// type given and the [`DDValue`]'s internal types are the same
    ///
    /// # Safety
    ///
    /// The type given must be the same as the [`DDValue`]'s internal type
    ///
    #[inline]
    unsafe fn from_ddvalue_unchecked(value: DDValue) -> Self
    where
        Self: 'static,
    {
        let value_type = (value.vtable.type_id)(&value.val);
        debug_assert_eq!(value_type, TypeId::of::<Self>());

        Self::try_from_ddvalue(value).unwrap_or_else(|_| unreachable_unchecked())
    }

    /// Convert a value to a `DDVal`, erasing its original type.
    ///
    /// This is a safe conversion that cannot fail.
    fn into_ddval(self) -> DDVal;

    /// Creates a `DDValue` from the current value
    fn ddvalue(&self) -> DDValue;

    /// Converts the current value into a `DDValue`
    fn into_ddvalue(self) -> DDValue;

    /// The vtable containing all `DDValue` methods for the current type
    const VTABLE: DDValMethods;

    /// Will be `true` if the current type can fit within a [`usize`]
    const FITS_IN_USIZE: bool =
        size_of::<Self>() <= size_of::<usize>() && align_of::<Self>() <= align_of::<usize>();
}

/// Implement `DDValConvert` for all types that satisfy its type constraints
impl<T> DDValConvert for T
where
    T: Any
        + Clone
        + Debug
        + IntoRecord
        + Eq
        + PartialEq
        + Ord
        + PartialOrd
        + Hash
        + Send
        + Sync
        + erased_serde::Serialize
        + 'static,
    Record: Mutator<T>,
{
    #[inline]
    unsafe fn from_ddval_ref(value: &DDVal) -> &Self {
        if Self::FITS_IN_USIZE {
            &*<*const usize>::cast::<Self>(&value.v)
        } else {
            &*(value.v as *const Self)
        }
    }

    #[inline]
    unsafe fn from_ddval(value: DDVal) -> Self {
        if Self::FITS_IN_USIZE {
            <*const usize>::cast::<Self>(&value.v).read()
        } else {
            let arc = Arc::from_raw(value.v as *const Self);

            // If the `Arc` is uniquely held then simply take the inner value,
            // otherwise clone it out
            Arc::try_unwrap(arc).unwrap_or_else(|arc| Self::clone(&*arc))
        }
    }

    #[inline]
    fn into_ddval(self) -> DDVal {
        // The size and alignment of the `T` must be less than or equal to a
        // `usize`'s, otherwise we store it within an `Arc`
        if Self::FITS_IN_USIZE {
            let mut v: usize = 0;
            unsafe { <*mut usize>::cast::<Self>(&mut v).write(self) };

            DDVal { v }
        } else {
            DDVal {
                v: Arc::into_raw(Arc::new(self)) as usize,
            }
        }
    }

    #[inline]
    fn ddvalue(&self) -> DDValue {
        DDValConvert::into_ddvalue(self.clone())
    }

    #[inline]
    fn into_ddvalue(self) -> DDValue {
        DDValue::new(self.into_ddval(), &Self::VTABLE)
    }

    const VTABLE: DDValMethods = {
        let clone = |this: &DDVal| -> DDVal {
            if Self::FITS_IN_USIZE {
                // Safety: The caller promises to ensure that Self
                //         is the current type of the given value
                let value = unsafe { Self::from_ddval_ref(this) };

                value.clone().into_ddval()
            } else {
                // Safety: `this.v` is a pointer created by `Arc::into_raw()`
                let arc = unsafe { ManuallyDrop::new(Arc::from_raw(this.v as *const Self)) };

                DDVal {
                    v: Arc::into_raw(Arc::clone(&arc)) as usize,
                }
            }
        };

        let into_record =
            |this: DDVal| -> Record { unsafe { Self::from_ddval(this) }.into_record() };

        let eq: unsafe fn(&DDVal, &DDVal) -> bool = |this, other| unsafe {
            // Safety: The caller promises to ensure that `this` and `other` have the same type
            let (this, other) = (Self::from_ddval_ref(this), Self::from_ddval_ref(other));

            this.eq(other)
        };

        let partial_cmp: unsafe fn(&DDVal, &DDVal) -> Option<Ordering> = |this, other| unsafe {
            // Safety: The caller promises to ensure that `this` and `other` have the same type
            let (this, other) = (Self::from_ddval_ref(this), Self::from_ddval_ref(other));

            this.partial_cmp(other)
        };

        let cmp: unsafe fn(&DDVal, &DDVal) -> Ordering = |this, other| unsafe {
            // Safety: The caller promises to ensure that `this` and `other` have the same type
            let (this, other) = (Self::from_ddval_ref(this), Self::from_ddval_ref(other));

            this.cmp(other)
        };

        let hash = |this: &DDVal, mut state: &mut dyn Hasher| {
            // Safety: The caller promises to ensure that Self
            //         is the current type of the given value
            let value = unsafe { Self::from_ddval_ref(this) };

            Hash::hash(value, &mut state);
        };

        let mutate = |this: &mut DDVal, record: &Record| -> Result<(), String> {
            // Safety: The caller promises to ensure that Self
            //         is the current type of the given value
            let mut clone = unsafe { Self::from_ddval_ref(this) }.clone();
            Mutator::mutate(record, &mut clone)?;
            *this = clone.into_ddval();

            Ok(())
        };

        let fmt_debug = |this: &DDVal, f: &mut Formatter| -> Result<(), fmt::Error> {
            // Safety: The caller promises to ensure that Self
            //         is the current type of the given value
            let value = unsafe { Self::from_ddval_ref(this) };

            Debug::fmt(value, f)
        };

        let fmt_display = |this: &DDVal, f: &mut Formatter| -> Result<(), fmt::Error> {
            // Safety: The caller promises to ensure that Self
            //         is the current type of the given value
            let value = unsafe { Self::from_ddval_ref(this) };

            Display::fmt(&value.clone().into_record(), f)
        };

        let drop = |this: &mut DDVal| {
            if Self::FITS_IN_USIZE {
                // If the current type needs dropping, do it. Otherwise, we can statically elide it
                if needs_drop::<Self>() {
                    // Allow the inner value's Drop impl to run automatically
                    let _val = unsafe { <*const usize>::cast::<Self>(&this.v).read() };
                }
            } else {
                let arc = unsafe { Arc::from_raw(this.v as *const Self) };
                mem::drop(arc);
            }
        };

        let ddval_serialize: fn(&DDVal) -> &dyn erased_serde::Serialize =
            |this| unsafe { Self::from_ddval_ref(this) as &dyn erased_serde::Serialize };

        let type_id = |_this: &DDVal| -> TypeId { TypeId::of::<Self>() };

        DDValMethods {
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
        }
    };
}

impl Default for DDValue {
    #[inline]
    fn default() -> Self {
        ().into_ddvalue()
    }
}
