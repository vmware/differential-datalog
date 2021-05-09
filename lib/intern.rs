/*
Copyright (c) 2021 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use differential_datalog::record::{self, Record};
use fxhash::FxBuildHasher;
use lasso::{Capacity, Key, Spur, ThreadedRodeo};
use once_cell::sync::Lazy;
use serde::{de::Deserializer, ser::Serializer};
use std::{
    fmt::{self, Debug, Display, Formatter},
    marker::PhantomData,
    num::NonZeroUsize,
};

pub static GLOBAL_STRING_INTERNER: Lazy<ThreadedRodeo<Spur, FxBuildHasher>> = Lazy::new(|| {
    // Safety: `NonZeroUsize::new_unchecked()` must be called with a non-zero
    //         integer, and 4096 is not zero
    ThreadedRodeo::with_capacity_and_hasher(
        Capacity::new(512, unsafe { NonZeroUsize::new_unchecked(4096) }),
        FxBuildHasher::default(),
    )
});

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct IObj<T> {
    key: Spur,
    __type: PhantomData<T>,
}

impl<T> IObj<T> {
    pub const fn new(key: Spur) -> Self {
        Self {
            key,
            __type: PhantomData,
        }
    }
}

impl Default for IString {
    fn default() -> Self {
        IObj::new(GLOBAL_STRING_INTERNER.get_or_intern_static(""))
    }
}

pub fn string_intern(string: &String) -> IString {
    IObj::new(GLOBAL_STRING_INTERNER.get_or_intern(string))
}

pub fn istring_str(string: &IString) -> String {
    GLOBAL_STRING_INTERNER.resolve(&string.key).to_owned()
}

pub fn istring_ord(string: &IString) -> u32 {
    string.key.into_usize() as u32
}

impl Display for IString {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let string = GLOBAL_STRING_INTERNER.resolve(&self.key);
        record::format_ddlog_str(string, f)
    }
}

impl Debug for IString {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let string = GLOBAL_STRING_INTERNER.resolve(&self.key);
        record::format_ddlog_str(string, f)
    }
}

impl Serialize for IString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let string = GLOBAL_STRING_INTERNER.resolve(&self.key);
        string.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for IString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|s| string_intern(&s))
    }
}

impl FromRecord for IString {
    fn from_record(val: &Record) -> Result<Self, String> {
        String::from_record(val).map(|s| string_intern(&s))
    }
}

impl IntoRecord for IString {
    fn into_record(self) -> Record {
        istring_str(&self).into_record()
    }
}

impl Mutator<IString> for Record {
    fn mutate(&self, s: &mut IString) -> Result<(), String> {
        let mut string = istring_str(s);
        self.mutate(&mut string)?;
        *s = string_intern(&string);

        Ok(())
    }
}
