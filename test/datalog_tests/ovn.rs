type Set<T> = FnvHashSet<T>;

fn build_dhcp_netmask(_cidr: &String) -> String {
    "not implemented: build_dhcp_netmask".to_string()
}
fn eth_addr_from_string(_str: &String) -> option_t<mac_addr_t> {
    option_t::None
}
fn extract_ips(_str: &String) -> Set<ip_addr_t> {
    FnvHashSet::default()
}

fn extract_mac(_str: &String) -> (mac_addr_t, String) {
    (0, "extract_mac not implemented".to_string())
}
fn extract_subnets(_str: &String) -> Set<ip_subnet_t> {
    FnvHashSet::default()
}

fn in6_generate_lla(_mac: &mac_addr_t) -> ip6_addr_t {
    0
}
fn ip_address_and_port_from_lb_key(_key: &String) -> ip_port_t {
    ip_port_t{ip: "0.0.0.0".to_string(), port: option_t::None}
}
fn ip_parse(_str: &String) -> option_t<ip_addr_t> {
    option_t::None
}

fn ipv6_string_mapped(_addr: &ip6_addr_t) -> String {
    "not implemented: ipv6_string_mapped".to_string()
}

