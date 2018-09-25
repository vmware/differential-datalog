type Set<T> = FnvHashSet<T>;

fn build_dhcp_netmask(_cidr: &String) -> arcval::DDString {
    arcval::ArcVal::<String>::from_str("not implemented: build_dhcp_netmask")
}
fn eth_addr_from_string(_str: &String) -> option_t<mac_addr_t> {
    option_t::None
}
fn extract_ips(_str: &arcval::DDString) -> Set<ip_addr_t> {
    FnvHashSet::default()
}

fn extract_mac(_str: &arcval::DDString) -> (mac_addr_t, arcval::DDString) {
    (0, arcval::DDString::from_str("extract_mac not implemented"))
}
fn extract_subnets(_str: &String) -> Set<ip_subnet_t> {
    FnvHashSet::default()
}

fn in6_generate_lla(_mac: &mac_addr_t) -> ip6_addr_t {
    0
}
fn ip_address_and_port_from_lb_key(_key: &arcval::DDString) -> ip_port_t {
    ip_port_t{ip: arcval::DDString::from_str("0.0.0.0"), port: option_t::None}
}
fn ip_parse(_str: &arcval::DDString) -> option_t<ip_addr_t> {
    option_t::None
}

fn ipv6_string_mapped(_addr: &ip6_addr_t) -> arcval::DDString {
    arcval::DDString::from_str("not implemented: ipv6_string_mapped")
}

