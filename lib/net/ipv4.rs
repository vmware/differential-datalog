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
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd)]
pub struct Ipv4Addr(::std::net::Ipv4Addr);

impl Ipv4Addr {
    pub fn new(addr: ::std::net::Ipv4Addr) -> Self {
        Ipv4Addr(addr)
    }
}

impl Deref for Ipv4Addr {
    type Target = ::std::net::Ipv4Addr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Ipv4Addr {
    fn default() -> Ipv4Addr {
        iPV4_UNSPECIFIED()
    }
}

impl fmt::Display for Ipv4Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ipv4_to_u32(self))
    }
}

impl fmt::Debug for Ipv4Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for Ipv4Addr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ipv4_to_u32(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Ipv4Addr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        u32::deserialize(deserializer).map(|x| ipv4_from_u32(&x))
    }
}

impl FromRecord for Ipv4Addr {
    fn from_record(val: &Record) -> Result<Self, String> {
        u32::from_record(val).map(|x| ipv4_from_u32(&x))
    }
}

impl IntoRecord for Ipv4Addr {
    fn into_record(self) -> Record {
        ipv4_to_u32(&self).into_record()
    }
}

impl Mutator<Ipv4Addr> for Record {
    fn mutate(&self, addr: &mut Ipv4Addr) -> Result<(), String> {
        Ipv4Addr::from_record(self).map(|a| *addr = a)
    }
}

pub fn ipv4_new(a: &u8, b: &u8, c: &u8, d: &u8) -> Ipv4Addr {
    Ipv4Addr(::std::net::Ipv4Addr::new(*a, *b, *c, *d))
}

pub fn ipv4_from_u32(ip: &u32) -> Ipv4Addr {
    Ipv4Addr(::std::net::Ipv4Addr::from(*ip))
}

pub fn ipv4_from_octet_vec(
    octets: &ddlog_std::Vec<u8>,
) -> ddlog_std::Option<Ipv4Addr> {
    if octets.len() != 4 {
        return ddlog_std::Option::None;
    };
    ddlog_std::Option::Some {
        x: Ipv4Addr::new(::std::net::Ipv4Addr::from([
            octets[0], octets[1], octets[2], octets[3],
        ])),
    }
}
pub fn ipv4_from_str(s: &String) -> ddlog_std::Result<Ipv4Addr, String> {
    ddlog_std::res2std(::std::net::Ipv4Addr::from_str(&*s).map(Ipv4Addr))
}

pub fn ipv4Addr2string(addr: &Ipv4Addr) -> String {
    (**addr).to_string()
}

pub fn iPV4_LOCALHOST() -> Ipv4Addr {
    Ipv4Addr(::std::net::Ipv4Addr::LOCALHOST)
}

pub fn iPV4_UNSPECIFIED() -> Ipv4Addr {
    Ipv4Addr(::std::net::Ipv4Addr::UNSPECIFIED)
}

pub fn iPV4_BROADCAST() -> Ipv4Addr {
    Ipv4Addr(::std::net::Ipv4Addr::BROADCAST)
}

pub fn ipv4_octets(addr: &Ipv4Addr) -> ddlog_std::tuple4<u8, u8, u8, u8> {
    let octets = addr.octets();
    ddlog_std::tuple4(octets[0], octets[1], octets[2], octets[3])
}

pub fn ipv4_octet_vec(addr: &Ipv4Addr) -> ddlog_std::Vec<u8> {
    ddlog_std::Vec::from(addr.octets().as_ref())
}

pub fn ipv4_is_unspecified(addr: &Ipv4Addr) -> bool {
    addr.is_unspecified()
}

pub fn ipv4_is_loopback(addr: &Ipv4Addr) -> bool {
    addr.is_loopback()
}

pub fn ipv4_is_private(addr: &Ipv4Addr) -> bool {
    addr.is_private()
}

pub fn ipv4_is_link_local(addr: &Ipv4Addr) -> bool {
    addr.is_link_local()
}

pub fn ipv4_is_multicast(addr: &Ipv4Addr) -> bool {
    addr.is_multicast()
}

pub fn ipv4_is_broadcast(addr: &Ipv4Addr) -> bool {
    addr.is_broadcast()
}

pub fn ipv4_is_documentation(addr: &Ipv4Addr) -> bool {
    addr.is_documentation()
}

pub fn ipv4_to_ipv6_compatible(addr: &Ipv4Addr) -> super::ipv6::Ipv6Addr {
    super::ipv6::Ipv6Addr::new(addr.to_ipv6_compatible())
}

pub fn ipv4_to_ipv6_mapped(addr: &Ipv4Addr) -> crate::ipv6::Ipv6Addr {
    super::ipv6::Ipv6Addr::new(addr.to_ipv6_mapped())
}

pub fn ipv4_to_u32(addr: &Ipv4Addr) -> u32 {
    u32::from(**addr)
}
