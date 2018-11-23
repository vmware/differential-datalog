use differential_datalog::arcval;
use differential_datalog::record;
use std::ffi;
use std::ptr;
use std::default;
use libc;

// TODO: proper implementation
pub fn ovn_warn(msg: &arcval::DDString) {
    warn(msg.as_ref())
}

const ETH_ADDR_SIZE:    usize = 6;
const IN6_ADDR_SIZE:    usize = 16;
const INET6_ADDRSTRLEN: usize = 46;
const INET_ADDRSTRLEN:  usize = 16;
const ETH_ADDR_STRLEN:  usize = 17;

/* Implementation for externs declared in ovn.dl */

#[repr(C)]
#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Serialize, Deserialize, Debug)]
pub struct ovn_eth_addr {
    x: [u8; ETH_ADDR_SIZE]
}

pub fn ovn_eth_addr_zero() -> ovn_eth_addr {
    ovn_eth_addr { x: [0; ETH_ADDR_SIZE] }
}

pub fn ovn_eth_addr2string(addr: &ovn_eth_addr) -> arcval::DDString {
    arcval::DDString::from(format!("{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                                   addr.x[0], addr.x[1], addr.x[2], addr.x[3], addr.x[4], addr.x[5]))
}

impl FromRecord for ovn_eth_addr {
    fn from_record(val: &record::Record) -> Result<Self, String> {
        Ok(ovn_eth_addr{x: <[u8; ETH_ADDR_SIZE]>::from_record(val)?})
    }
}

decl_struct_into_record!(ovn_eth_addr, <>, x);

#[repr(C)]
#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Serialize, Deserialize, Debug)]
pub struct ovn_in6_addr {
    x: [u8; IN6_ADDR_SIZE]
}

impl FromRecord for ovn_in6_addr {
    fn from_record(val: &record::Record) -> Result<Self, String> {
        Ok(ovn_in6_addr{x: <[u8; IN6_ADDR_SIZE]>::from_record(val)?})
    }
}

decl_struct_into_record!(ovn_in6_addr, <>, x);

pub fn ovn_in6_generate_lla(ea: &ovn_eth_addr) -> ovn_in6_addr {
    let mut addr: ovn_in6_addr = Default::default();
    unsafe {export_in6_generate_lla(ea.clone(), &mut addr as *mut ovn_in6_addr)};
    addr
}

pub fn ovn_ipv6_string_mapped(addr: &ovn_in6_addr) -> arcval::DDString {
    let mut addr_str = [0 as i8; INET6_ADDRSTRLEN];
    unsafe {
        ipv6_string_mapped(&mut addr_str[0] as *mut raw::c_char, addr as *const ovn_in6_addr);
        cstr2ddstring(&addr_str as *const raw::c_char)
    }
}

pub fn ovn_json_string_escape(s: &arcval::DDString) -> arcval::DDString {
    let mut ds = ovs_ds::new();
    unsafe {
        json_string_escape(ffi::CString::new(s.str()).unwrap().as_ptr() as *const raw::c_char,
                           &mut ds as *mut ovs_ds);
    };
    arcval::DDString::from(unsafe{ds.into_string()})
}

pub fn ovn_extract_lsp_addresses(address: &arcval::DDString) -> std_Option<ovn_lport_addresses> {
    unsafe {
        let mut laddrs: lport_addresses = Default::default();
        if extract_lsp_addresses(ffi::CString::new(address.str()).unwrap().as_ptr() as *const raw::c_char,
        &mut laddrs as *mut lport_addresses) {
            std_Option::std_Some{x: laddrs.into_ddlog()}
        } else {
            std_Option::std_None
        }
    }
}

/* Internals */

unsafe fn cstr2string(s: *const raw::c_char) -> String {
    ffi::CStr::from_ptr(s).to_owned().into_string().
        unwrap_or_else(|e|{ warn(format!("cstr2string: {}", e).as_ref()); "".to_owned() })
}

unsafe fn cstr2ddstring(s: *const raw::c_char) -> arcval::DDString {
    arcval::DDString::from(cstr2string(s))
}

fn warn(msg: &str) {
    eprintln!("{}", msg)
}

/* OVS dynamic string type */
#[repr(C)]
struct ovs_ds {
    s: *mut raw::c_char,       /* Null-terminated string. */
    length: libc::size_t,      /* Bytes used, not including null terminator. */
    allocated: libc::size_t    /* Bytes allocated, not including null terminator. */
}

impl ovs_ds {
    pub fn new() -> ovs_ds {
        ovs_ds{s: ptr::null_mut(), length: 0, allocated: 0}
    }

    pub unsafe fn into_string(mut self) -> String {
        let res = cstr2string(ds_cstr(&self as *const ovs_ds));
        ds_destroy(&mut self as *mut ovs_ds);
        res
    }
}


// ovn/lib/ovn-util.h
#[repr(C)]
struct ipv4_netaddr {
    addr:       libc::uint32_t,
    mask:       libc::uint32_t,
    network:    libc::uint32_t,
    plen:       raw::c_uint,

    addr_s:     [raw::c_char; INET_ADDRSTRLEN + 1],  /* "192.168.10.123" */
    network_s:  [raw::c_char; INET_ADDRSTRLEN + 1],  /* "192.168.10.0" */
    bcast_s:    [raw::c_char; INET_ADDRSTRLEN + 1]   /* "192.168.10.255" */
}

impl Default for ipv4_netaddr {
    fn default() -> Self {
        ipv4_netaddr {
            addr:       0,
            mask:       0,
            network:    0,
            plen:       0,
            addr_s:     [0; INET_ADDRSTRLEN + 1],
            network_s:  [0; INET_ADDRSTRLEN + 1],
            bcast_s:    [0; INET_ADDRSTRLEN + 1]
        }
    }
}

impl ipv4_netaddr {
    pub unsafe fn to_ddlog(&self) -> ovn_ipv4_netaddr {
        ovn_ipv4_netaddr{
            addr:       self.addr,
            mask:       self.mask,
            network:    self.network,
            plen:       self.plen,
            addr_s:     cstr2ddstring(&self.addr_s as *const raw::c_char),
            network_s:  cstr2ddstring(&self.network_s as *const raw::c_char),
            bcast_s:    cstr2ddstring(&self.bcast_s as *const raw::c_char)
        }
    }
}

#[repr(C)]
struct ipv6_netaddr {
    addr:       ovn_in6_addr,     /* fc00::1 */
    mask:       ovn_in6_addr,     /* ffff:ffff:ffff:ffff:: */
    sn_addr:    ovn_in6_addr,     /* ff02:1:ff00::1 */
    network:    ovn_in6_addr,     /* fc00:: */
    plen:       raw::c_uint,      /* CIDR Prefix: 64 */

    addr_s:     [raw::c_char; INET6_ADDRSTRLEN + 1],    /* "fc00::1" */
    sn_addr_s:  [raw::c_char; INET6_ADDRSTRLEN + 1],    /* "ff02:1:ff00::1" */
    network_s:  [raw::c_char; INET6_ADDRSTRLEN + 1]     /* "fc00::" */
}

impl Default for ipv6_netaddr {
    fn default() -> Self {
        ipv6_netaddr {
            addr:       Default::default(),
            mask:       Default::default(),
            sn_addr:    Default::default(),
            network:    Default::default(),
            plen:       0,
            addr_s:     [0; INET6_ADDRSTRLEN + 1],
            sn_addr_s:  [0; INET6_ADDRSTRLEN + 1],
            network_s:  [0; INET6_ADDRSTRLEN + 1]
        }
    }
}

impl ipv6_netaddr {
    pub unsafe fn to_ddlog(&self) -> ovn_ipv6_netaddr {
        ovn_ipv6_netaddr{
            addr:       self.addr.clone(),
            mask:       self.mask.clone(),
            sn_addr:    self.sn_addr.clone(),
            network:    self.network.clone(),
            plen:       self.plen,
            addr_s:     cstr2ddstring(&self.addr_s as *const raw::c_char),
            sn_addr_s:  cstr2ddstring(&self.sn_addr_s as *const raw::c_char),
            network_s:  cstr2ddstring(&self.network_s as *const raw::c_char)
        }
    }
}


// ovn-util.h
#[repr(C)]
struct lport_addresses {
    ea_s:           [raw::c_char; ETH_ADDR_STRLEN + 1],
    ea:             ovn_eth_addr,
    n_ipv4_addrs:   libc::size_t,
    ipv4_addrs:     *mut ipv4_netaddr,
    n_ipv6_addrs:   libc::size_t,
    ipv6_addrs:     *mut ipv6_netaddr
}

impl Default for lport_addresses {
    fn default() -> Self {
        lport_addresses {
            ea_s:           [0; ETH_ADDR_STRLEN + 1],
            ea:             Default::default(),
            n_ipv4_addrs:   0,
            ipv4_addrs:     ptr::null_mut(),
            n_ipv6_addrs:   0,
            ipv6_addrs:     ptr::null_mut()
        }
    }
}

impl lport_addresses {
    pub unsafe fn into_ddlog(mut self) -> ovn_lport_addresses {
        let mut ipv4_addrs = std_Vec::with_capacity(self.n_ipv4_addrs);
        for i in 0..self.n_ipv4_addrs {
            ipv4_addrs.push((&*self.ipv4_addrs.offset(i as isize)).to_ddlog())
        }
        let mut ipv6_addrs = std_Vec::with_capacity(self.n_ipv6_addrs);
        for i in 0..self.n_ipv6_addrs {
            ipv6_addrs.push((&*self.ipv6_addrs.offset(i as isize)).to_ddlog())
        }
        let res = ovn_lport_addresses {
            ea_s:       cstr2ddstring(&self.ea_s as *const raw::c_char),
            ea:         self.ea.clone(),
            ipv4_addrs: ipv4_addrs,
            ipv6_addrs: ipv6_addrs
        };
        destroy_lport_addresses(&mut self as *mut lport_addresses);
        res
    }
}

/* functions imported from libovn */
#[link(name = "ovn")]
extern "C" {
    // ovn/lib/ovn-util.h
    fn extract_lsp_addresses(address: *const raw::c_char, laddrs: *mut lport_addresses) -> bool;
    fn destroy_lport_addresses(addrs: *mut lport_addresses);
}

/* functions imported from libopenvswitch */
#[link(name = "openvswitch")]
extern "C" {
    fn ipv6_string_mapped(addr_str: *mut raw::c_char, addr: *const ovn_in6_addr) -> *const raw::c_char;
    // include/openvswitch/json.h
    fn json_string_escape(str: *const raw::c_char, out: *mut ovs_ds);
    // openvswitch/dynamic-string.h
    fn ds_destroy(ds: *mut ovs_ds);
    fn ds_cstr(ds: *const ovs_ds) -> *const raw::c_char;
}

extern "C" {
    // lib/packets.h
    fn export_in6_generate_lla(ea: ovn_eth_addr, lla: *mut ovn_in6_addr);
}
