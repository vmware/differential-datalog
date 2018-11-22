extern crate libtool;

fn main() {
    libtool::generate_convenience_lib("libdatalog_example_ddlog").unwrap();
}
