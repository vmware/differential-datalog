use differential_datalog::record::FromRecord;
use differential_datalog::record::IntoRecord;
use differential_datalog::record::Mutator;
use differential_datalog::record::Record;
use serde::de::Deserialize;
use serde::de::Deserializer;
use serde::ser::Serialize;
use serde::ser::Serializer;
use std::net::Ipv4Addr;
use std::str::FromStr;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd)]
pub struct net_ipv4_Ipv4Addr(Ipv4Addr);

impl net_ipv4_Ipv4Addr {
    pub fn new(addr: Ipv4Addr) -> Self {
        net_ipv4_Ipv4Addr(addr)
    }
}

impl Deref for net_ipv4_Ipv4Addr {
    type Target = Ipv4Addr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for net_ipv4_Ipv4Addr {
    fn default() -> net_ipv4_Ipv4Addr {
        net_ipv4_iPV4_UNSPECIFIED()
    }
}

impl fmt::Display for net_ipv4_Ipv4Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", net_ipv4_ipv4_to_u32(self))
    }
}

impl fmt::Debug for net_ipv4_Ipv4Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for net_ipv4_Ipv4Addr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        net_ipv4_ipv4_to_u32(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for net_ipv4_Ipv4Addr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        u32::deserialize(deserializer).map(|x| net_ipv4_ipv4_from_u32(&x))
    }
}

impl FromRecord for net_ipv4_Ipv4Addr {
    fn from_record(val: &Record) -> Result<Self, String> {
        u32::from_record(val).map(|x| net_ipv4_ipv4_from_u32(&x))
    }
}

impl IntoRecord for net_ipv4_Ipv4Addr {
    fn into_record(self) -> Record {
        net_ipv4_ipv4_to_u32(&self).into_record()
    }
}

impl Mutator<net_ipv4_Ipv4Addr> for Record {
    fn mutate(&self, addr: &mut net_ipv4_Ipv4Addr) -> Result<(), String> {
        net_ipv4_Ipv4Addr::from_record(self).map(|a| *addr = a)
    }
}

pub fn net_ipv4_ipv4_new(a: &u8, b: &u8, c: &u8, d: &u8) -> net_ipv4_Ipv4Addr {
    net_ipv4_Ipv4Addr(Ipv4Addr::new(*a, *b, *c, *d))
}

pub fn net_ipv4_ipv4_from_u32(ip: &u32) -> net_ipv4_Ipv4Addr {
    net_ipv4_Ipv4Addr(Ipv4Addr::from(*ip))
}

pub fn net_ipv4_ipv4_from_octet_vec(octets: &std_Vec<u8>) -> std_Option<net_ipv4_Ipv4Addr> {
    if octets.len() != 4 {
        return std_Option::std_None;
    };
    std_Option::std_Some {
        x: net_ipv4_Ipv4Addr::new(Ipv4Addr::from([octets[0], octets[1], octets[2], octets[3]])),
    }
}
pub fn net_ipv4_ipv4_from_str(s: &String) -> std_Result<net_ipv4_Ipv4Addr, String> {
    res2std(Ipv4Addr::from_str(&*s).map(net_ipv4_Ipv4Addr))
}

pub fn net_ipv4_ipv4Addr2string(addr: &net_ipv4_Ipv4Addr) -> String {
    (**addr).to_string()
}

pub fn net_ipv4_iPV4_LOCALHOST() -> net_ipv4_Ipv4Addr {
    net_ipv4_Ipv4Addr(Ipv4Addr::LOCALHOST)
}

pub fn net_ipv4_iPV4_UNSPECIFIED() -> net_ipv4_Ipv4Addr {
    net_ipv4_Ipv4Addr(Ipv4Addr::UNSPECIFIED)
}

pub fn net_ipv4_iPV4_BROADCAST() -> net_ipv4_Ipv4Addr {
    net_ipv4_Ipv4Addr(Ipv4Addr::BROADCAST)
}

pub fn net_ipv4_ipv4_octets(addr: &net_ipv4_Ipv4Addr) -> (u8, u8, u8, u8) {
    let octets = addr.octets();
    (octets[0], octets[1], octets[2], octets[3])
}

pub fn net_ipv4_ipv4_octet_vec(addr: &net_ipv4_Ipv4Addr) -> std_Vec<u8> {
    std_Vec::from(addr.octets().as_ref())
}

pub fn net_ipv4_ipv4_is_unspecified(addr: &net_ipv4_Ipv4Addr) -> bool {
    addr.is_unspecified()
}

pub fn net_ipv4_ipv4_is_loopback(addr: &net_ipv4_Ipv4Addr) -> bool {
    addr.is_loopback()
}

pub fn net_ipv4_ipv4_is_private(addr: &net_ipv4_Ipv4Addr) -> bool {
    addr.is_private()
}

pub fn net_ipv4_ipv4_is_link_local(addr: &net_ipv4_Ipv4Addr) -> bool {
    addr.is_link_local()
}

pub fn net_ipv4_ipv4_is_multicast(addr: &net_ipv4_Ipv4Addr) -> bool {
    addr.is_multicast()
}

pub fn net_ipv4_ipv4_is_broadcast(addr: &net_ipv4_Ipv4Addr) -> bool {
    addr.is_broadcast()
}

pub fn net_ipv4_ipv4_is_documentation(addr: &net_ipv4_Ipv4Addr) -> bool {
    addr.is_documentation()
}

pub fn net_ipv4_ipv4_to_ipv6_compatible(addr: &net_ipv4_Ipv4Addr) -> net_ipv6_Ipv6Addr {
    net_ipv6_Ipv6Addr::new(addr.to_ipv6_compatible())
}

pub fn net_ipv4_ipv4_to_ipv6_mapped(addr: &net_ipv4_Ipv4Addr) -> net_ipv6_Ipv6Addr {
    net_ipv6_Ipv6Addr::new(addr.to_ipv6_mapped())
}

pub fn net_ipv4_ipv4_to_u32(addr: &net_ipv4_Ipv4Addr) -> u32 {
    u32::from(**addr)
}
