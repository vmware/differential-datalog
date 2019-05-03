extern crate libtool;

use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let lib = "libdatalog_example_ddlog";

    /* Start: fixup for a bug in libtool, which does not correctly
     * remove the symlink it creates.  Remove this fixup once an updated
     * libtool crate is available.
     *
     * See: https://github.com/kanru/libtool-rs/issues/2#issue-440212008
     *
     * */
    let topdir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let profile = env::var("PROFILE").unwrap();
    let target_dir = format!("{}/target/{}", topdir, profile);
    let libs_dir = format!("{}/.libs", target_dir);
    let new_lib_path = PathBuf::from(format!("{}/{}.a", libs_dir, lib));
    fs::remove_file(&new_lib_path);
    /* End: fixup */

    libtool::generate_convenience_lib(lib).unwrap();
}
