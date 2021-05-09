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

use differential_datalog::record::Record;
use serde::de::Deserializer;
use serde::ser::Serializer;
use std::default::Default;
use std::fmt;
pub use uuid as crate_uuid;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd)]
pub struct Uuid(::uuid::Uuid);

impl Uuid {
    pub fn new(addr: ::uuid::Uuid) -> Self {
        Uuid(addr)
    }
}

impl Deref for Uuid {
    type Target = ::uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Uuid {
    fn default() -> Uuid {
        nil()
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", as_u128(self))
    }
}

impl fmt::Debug for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for Uuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        as_u128(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Uuid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        u128::deserialize(deserializer).map(|x| from_u128(&x))
    }
}

impl FromRecord for Uuid {
    fn from_record(val: &Record) -> Result<Self, String> {
        u128::from_record(val).map(|x| from_u128(&x))
    }
}

impl IntoRecord for Uuid {
    fn into_record(self) -> Record {
        as_u128(&self).into_record()
    }
}

impl Mutator<Uuid> for Record {
    fn mutate(&self, uuid: &mut Uuid) -> Result<(), String> {
        Uuid::from_record(self).map(|u| *uuid = u)
    }
}

pub fn nil() -> Uuid {
    Uuid::new(::uuid::Uuid::nil())
}

pub fn nAMESPACE_DNS() -> Uuid {
    Uuid::new(::uuid::Uuid::NAMESPACE_DNS)
}

pub fn nAMESPACE_OID() -> Uuid {
    Uuid::new(::uuid::Uuid::NAMESPACE_OID)
}

pub fn nAMESPACE_URL() -> Uuid {
    Uuid::new(::uuid::Uuid::NAMESPACE_URL)
}

pub fn nAMESPACE_X500() -> Uuid {
    Uuid::new(::uuid::Uuid::NAMESPACE_X500)
}

pub fn new_v5(namespace: &Uuid, name: &ddlog_std::Vec<u8>) -> Uuid {
    Uuid::new(::uuid::Uuid::new_v5(namespace, &**name))
}

pub fn from_u128(v: &u128) -> Uuid {
    Uuid::new(::uuid::Uuid::from_u128(*v))
}

pub fn from_u128_le(v: &u128) -> Uuid {
    Uuid::new(::uuid::Uuid::from_u128_le(*v))
}

pub fn from_bytes(b: &ddlog_std::Vec<u8>) -> ddlog_std::Result<Uuid, Error> {
    ddlog_std::res2std(::uuid::Uuid::from_slice(&**b).map(Uuid::new))
}

pub fn parse_str(s: &String) -> ddlog_std::Result<Uuid, Error> {
    ddlog_std::res2std(::uuid::Uuid::parse_str(&s).map(Uuid::new))
}

pub fn to_hyphenated_lower(uuid: &Uuid) -> String {
    uuid.to_hyphenated_ref()
        .encode_lower(&mut ::uuid::Uuid::encode_buffer())
        .to_string()
}

pub fn to_hyphenated_upper(uuid: &Uuid) -> String {
    uuid.to_hyphenated_ref()
        .encode_upper(&mut ::uuid::Uuid::encode_buffer())
        .to_string()
}

pub fn to_simple_lower(uuid: &Uuid) -> String {
    uuid.to_simple_ref()
        .encode_lower(&mut ::uuid::Uuid::encode_buffer())
        .to_string()
}

pub fn to_simple_upper(uuid: &Uuid) -> String {
    uuid.to_simple_ref()
        .encode_upper(&mut ::uuid::Uuid::encode_buffer())
        .to_string()
}

pub fn to_urn_lower(uuid: &Uuid) -> String {
    uuid.to_urn_ref()
        .encode_lower(&mut ::uuid::Uuid::encode_buffer())
        .to_string()
}

pub fn to_urn_upper(uuid: &Uuid) -> String {
    uuid.to_urn_ref()
        .encode_upper(&mut ::uuid::Uuid::encode_buffer())
        .to_string()
}

pub fn as_u128(uuid: &Uuid) -> u128 {
    uuid.as_u128()
}

pub fn to_u128_le(uuid: &Uuid) -> u128 {
    uuid.to_u128_le()
}

pub fn is_nil(uuid: &Uuid) -> bool {
    uuid.is_nil()
}
