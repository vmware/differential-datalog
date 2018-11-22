use differential_datalog::arcval;

// TODO: proper implementation
pub fn ovn_warn(msg: &arcval::DDString) {
    eprintln!("{}", msg)
}
