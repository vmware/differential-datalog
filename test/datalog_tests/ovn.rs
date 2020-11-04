use fnv::FnvHashSet;

pub type Set<T> = FnvHashSet<T>;

pub fn build_dhcp_netmask(_cidr: &String) -> String {
    "not implemented: build_dhcp_netmask".to_owned()
}
pub fn eth_addr_from_string(_str: &String) -> ddlog_std::Option<mac_addr_t> {
    ddlog_std::Option::None
}
pub fn extract_ips(_str: &String) -> Set<ip_addr_t> {
    FnvHashSet::default()
}

pub fn extract_mac(_str: &String) -> ddlog_std::tuple2<mac_addr_t, String> {
    ddlog_std::tuple2(0, "extract_mac not implemented".to_owned())
}
pub fn extract_subnets(_str: &String) -> Set<ip_subnet_t> {
    FnvHashSet::default()
}

pub fn in6_generate_lla(_mac: &mac_addr_t) -> ip6_addr_t {
    0
}
pub fn ip_address_and_port_from_lb_key(_key: &String) -> ip_port_t {
    ip_port_t{ip: "0.0.0.0".to_owned(), port: ddlog_std::Option::None}
}
pub fn ip_parse(_str: &String) -> ddlog_std::Option<ip_addr_t> {
    ddlog_std::Option::None
}

pub fn ipv6_string_mapped(_addr: &ip6_addr_t) -> String {
    "not implemented: ipv6_string_mapped".to_owned()
}

