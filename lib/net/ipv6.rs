use differential_datalog::record::FromRecord;
use differential_datalog::record::IntoRecord;
use differential_datalog::record::Mutator;
use differential_datalog::record::Record;
use serde::de::Deserialize;
use serde::de::Deserializer;
use serde::ser::Serialize;
use serde::ser::Serializer;
use std::net::Ipv6Addr;
use std::str::FromStr;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd)]
pub struct net_ipv6_Ipv6Addr(Ipv6Addr);

impl net_ipv6_Ipv6Addr {
    pub fn new(addr: Ipv6Addr) -> Self {
        net_ipv6_Ipv6Addr(addr)
    }
}

impl Deref for net_ipv6_Ipv6Addr {
    type Target = Ipv6Addr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for net_ipv6_Ipv6Addr {
    fn default() -> net_ipv6_Ipv6Addr {
        net_ipv6_iPV6_UNSPECIFIED()
    }
}

impl fmt::Display for net_ipv6_Ipv6Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", net_ipv6_ipv6_to_u128(self))
    }
}

impl fmt::Debug for net_ipv6_Ipv6Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for net_ipv6_Ipv6Addr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        net_ipv6_ipv6_to_u128(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for net_ipv6_Ipv6Addr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        u128::deserialize(deserializer).map(|x| net_ipv6_ipv6_from_u128(&x))
    }
}

impl FromRecord for net_ipv6_Ipv6Addr {
    fn from_record(val: &Record) -> Result<Self, String> {
        u128::from_record(val).map(|x| net_ipv6_ipv6_from_u128(&x))
    }
}

impl IntoRecord for net_ipv6_Ipv6Addr {
    fn into_record(self) -> Record {
        net_ipv6_ipv6_to_u128(&self).into_record()
    }
}

impl Mutator<net_ipv6_Ipv6Addr> for Record {
    fn mutate(&self, addr: &mut net_ipv6_Ipv6Addr) -> Result<(), String> {
        net_ipv6_Ipv6Addr::from_record(self).map(|a| *addr = a)
    }
}

pub fn net_ipv6_ipv6_new(
    a: &u16,
    b: &u16,
    c: &u16,
    d: &u16,
    e: &u16,
    f: &u16,
    g: &u16,
    h: &u16,
) -> net_ipv6_Ipv6Addr {
    net_ipv6_Ipv6Addr::new(Ipv6Addr::new(*a, *b, *c, *d, *e, *f, *g, *h))
}

pub fn net_ipv6_ipv6_from_segment_vec(segments: &std_Vec<u16>) -> std_Option<net_ipv6_Ipv6Addr> {
    if segments.len() != 8 {
        return std_Option::std_None;
    };
    std_Option::std_Some {
        x: net_ipv6_Ipv6Addr::new(Ipv6Addr::from([
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

pub fn net_ipv6_ipv6_from_octets(
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
) -> net_ipv6_Ipv6Addr {
    net_ipv6_Ipv6Addr::new(Ipv6Addr::from([
        *b0, *b1, *b2, *b3, *b4, *b5, *b6, *b7, *b8, *b9, *b10, *b11, *b12, *b13, *b14, *b15,
    ]))
}

pub fn net_ipv6_ipv6_from_octet_vec(octets: &std_Vec<u8>) -> std_Option<net_ipv6_Ipv6Addr> {
    if octets.len() != 16 {
        return std_Option::std_None;
    };
    std_Option::std_Some {
        x: net_ipv6_Ipv6Addr::new(Ipv6Addr::from([
            octets[0], octets[1], octets[2], octets[3], octets[4], octets[5], octets[6], octets[7],
            octets[8], octets[9], octets[10], octets[11], octets[12], octets[13], octets[14],
            octets[15],
        ])),
    }
}

pub fn net_ipv6_ipv6_from_u128(ip: &u128) -> net_ipv6_Ipv6Addr {
    net_ipv6_Ipv6Addr::new(Ipv6Addr::from(*ip))
}

pub fn net_ipv6_ipv6_from_str(s: &String) -> std_Result<net_ipv6_Ipv6Addr, String> {
    res2std(Ipv6Addr::from_str(&*s).map(net_ipv6_Ipv6Addr::new))
}

pub fn net_ipv6_ipv6Addr2string(addr: &net_ipv6_Ipv6Addr) -> String {
    (**addr).to_string()
}

pub fn net_ipv6_ipv6_to_u128(addr: &net_ipv6_Ipv6Addr) -> u128 {
    u128::from(**addr)
}

pub fn net_ipv6_iPV6_LOCALHOST() -> net_ipv6_Ipv6Addr {
    net_ipv6_Ipv6Addr::new(Ipv6Addr::LOCALHOST)
}

pub fn net_ipv6_iPV6_UNSPECIFIED() -> net_ipv6_Ipv6Addr {
    net_ipv6_Ipv6Addr::new(Ipv6Addr::UNSPECIFIED)
}

pub fn net_ipv6_ipv6_segments(
    addr: &net_ipv6_Ipv6Addr,
) -> (u16, u16, u16, u16, u16, u16, u16, u16) {
    let segments = addr.segments();
    (
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

pub fn net_ipv6_ipv6_segment_vec(addr: &net_ipv6_Ipv6Addr) -> std_Vec<u16> {
    std_Vec::from(addr.segments().as_ref())
}

pub fn net_ipv6_ipv6_octets(
    addr: &net_ipv6_Ipv6Addr,
) -> tuple16<u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8> {
    let octets = addr.octets();
    tuple16(
        octets[0], octets[1], octets[2], octets[3], octets[4], octets[5], octets[6], octets[7],
        octets[8], octets[9], octets[10], octets[11], octets[12], octets[13], octets[14],
        octets[15],
    )
}

pub fn net_ipv6_ipv6_octet_vec(addr: &net_ipv6_Ipv6Addr) -> std_Vec<u8> {
    std_Vec::from(addr.octets().as_ref())
}

pub fn net_ipv6_ipv6_is_unspecified(addr: &net_ipv6_Ipv6Addr) -> bool {
    addr.is_unspecified()
}

pub fn net_ipv6_ipv6_is_loopback(addr: &net_ipv6_Ipv6Addr) -> bool {
    addr.is_loopback()
}

pub fn net_ipv6_ipv6_is_multicast(addr: &net_ipv6_Ipv6Addr) -> bool {
    addr.is_multicast()
}

pub fn net_ipv6_ipv6_to_ipv4(addr: &net_ipv6_Ipv6Addr) -> std_Option<net_ipv4_Ipv4Addr> {
    option2std(addr.to_ipv4().map(net_ipv4_Ipv4Addr::new))
}
