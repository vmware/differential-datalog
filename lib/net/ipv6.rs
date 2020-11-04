use differential_datalog::record::Record;
use serde::de::Deserializer;
use serde::ser::Serializer;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

use ddlog_std::option2std;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd)]
pub struct Ipv6Addr(::std::net::Ipv6Addr);

impl Ipv6Addr {
    pub fn new(addr: ::std::net::Ipv6Addr) -> Self {
        Ipv6Addr(addr)
    }
}

impl Deref for Ipv6Addr {
    type Target = ::std::net::Ipv6Addr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Ipv6Addr {
    fn default() -> Ipv6Addr {
        iPV6_UNSPECIFIED()
    }
}

impl fmt::Display for Ipv6Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ipv6_to_u128(self))
    }
}

impl fmt::Debug for Ipv6Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for Ipv6Addr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ipv6_to_u128(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Ipv6Addr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        u128::deserialize(deserializer).map(|x| ipv6_from_u128(&x))
    }
}

impl FromRecord for Ipv6Addr {
    fn from_record(val: &Record) -> Result<Self, String> {
        u128::from_record(val).map(|x| ipv6_from_u128(&x))
    }
}

impl IntoRecord for Ipv6Addr {
    fn into_record(self) -> Record {
        ipv6_to_u128(&self).into_record()
    }
}

impl Mutator<Ipv6Addr> for Record {
    fn mutate(&self, addr: &mut Ipv6Addr) -> Result<(), String> {
        Ipv6Addr::from_record(self).map(|a| *addr = a)
    }
}

pub fn ipv6_new(
    a: &u16,
    b: &u16,
    c: &u16,
    d: &u16,
    e: &u16,
    f: &u16,
    g: &u16,
    h: &u16,
) -> Ipv6Addr {
    Ipv6Addr::new(::std::net::Ipv6Addr::new(*a, *b, *c, *d, *e, *f, *g, *h))
}

pub fn ipv6_from_segment_vec(
    segments: &ddlog_std::Vec<u16>,
) -> ddlog_std::Option<Ipv6Addr> {
    if segments.len() != 8 {
        return ddlog_std::Option::None;
    };
    ddlog_std::Option::Some {
        x: Ipv6Addr::new(::std::net::Ipv6Addr::from([
            segments[0],
            segments[1],
            segments[2],
            segments[3],
            segments[4],
            segments[5],
            segments[6],
            segments[7],
        ])),
    }
}

pub fn ipv6_from_octets(
    b0: &u8,
    b1: &u8,
    b2: &u8,
    b3: &u8,
    b4: &u8,
    b5: &u8,
    b6: &u8,
    b7: &u8,
    b8: &u8,
    b9: &u8,
    b10: &u8,
    b11: &u8,
    b12: &u8,
    b13: &u8,
    b14: &u8,
    b15: &u8,
) -> Ipv6Addr {
    Ipv6Addr::new(::std::net::Ipv6Addr::from([
        *b0, *b1, *b2, *b3, *b4, *b5, *b6, *b7, *b8, *b9, *b10, *b11, *b12, *b13, *b14, *b15,
    ]))
}

pub fn ipv6_from_octet_vec(
    octets: &ddlog_std::Vec<u8>,
) -> ddlog_std::Option<Ipv6Addr> {
    if octets.len() != 16 {
        return ddlog_std::Option::None;
    };
    ddlog_std::Option::Some {
        x: Ipv6Addr::new(::std::net::Ipv6Addr::from([
            octets[0], octets[1], octets[2], octets[3], octets[4], octets[5], octets[6], octets[7],
            octets[8], octets[9], octets[10], octets[11], octets[12], octets[13], octets[14],
            octets[15],
        ])),
    }
}

pub fn ipv6_from_u128(ip: &u128) -> Ipv6Addr {
    Ipv6Addr::new(::std::net::Ipv6Addr::from(*ip))
}

pub fn ipv6_from_str(s: &String) -> ddlog_std::Result<Ipv6Addr, String> {
    ddlog_std::res2std(::std::net::Ipv6Addr::from_str(&*s).map(Ipv6Addr::new))
}

pub fn ipv6Addr2string(addr: &Ipv6Addr) -> String {
    (**addr).to_string()
}

pub fn ipv6_to_u128(addr: &Ipv6Addr) -> u128 {
    u128::from(**addr)
}

pub fn iPV6_LOCALHOST() -> Ipv6Addr {
    Ipv6Addr::new(::std::net::Ipv6Addr::LOCALHOST)
}

pub fn iPV6_UNSPECIFIED() -> Ipv6Addr {
    Ipv6Addr::new(::std::net::Ipv6Addr::UNSPECIFIED)
}

pub fn ipv6_segments(addr: &Ipv6Addr) -> ddlog_std::tuple8<u16, u16, u16, u16, u16, u16, u16, u16> {
    let segments = addr.segments();
    ddlog_std::tuple8(
        segments[0],
        segments[1],
        segments[2],
        segments[3],
        segments[4],
        segments[5],
        segments[6],
        segments[7],
    )
}

pub fn ipv6_segment_vec(addr: &Ipv6Addr) -> ddlog_std::Vec<u16> {
    ddlog_std::Vec::from(addr.segments().as_ref())
}

pub fn ipv6_octets(
    addr: &Ipv6Addr,
) -> ddlog_std::tuple16<u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8> {
    let octets = addr.octets();
    ddlog_std::tuple16(
        octets[0], octets[1], octets[2], octets[3], octets[4], octets[5], octets[6], octets[7],
        octets[8], octets[9], octets[10], octets[11], octets[12], octets[13], octets[14],
        octets[15],
    )
}

pub fn ipv6_octet_vec(addr: &Ipv6Addr) -> ddlog_std::Vec<u8> {
    ddlog_std::Vec::from(addr.octets().as_ref())
}

pub fn ipv6_is_unspecified(addr: &Ipv6Addr) -> bool {
    addr.is_unspecified()
}

pub fn ipv6_is_loopback(addr: &Ipv6Addr) -> bool {
    addr.is_loopback()
}

pub fn ipv6_is_multicast(addr: &Ipv6Addr) -> bool {
    addr.is_multicast()
}

pub fn ipv6_to_ipv4(addr: &Ipv6Addr) -> ddlog_std::Option<super::ipv4::Ipv4Addr> {
    option2std(addr.to_ipv4().map(super::ipv4::Ipv4Addr::new))
}
