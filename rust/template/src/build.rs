fn main() {
    #[cfg(not(windows))]
    libtool();
}

#[cfg(not(windows))]
fn libtool() {
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};

    let topdir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let fbufpath = format!("{}/src/flatbuf.rs", topdir);

    /* When `build.rs` is present, Cargo triggers recompilation whenever any
     * file in the Rust project directory changes, including auto-generated
     * Java files.  As a workaround, the following commands tell cargo to
     * only recompile when one of the listed files changes. */

    /* Only include flatbuf files as dependencies if the project was
     * compiled with flatbuf support.
     */
    if Path::new(fbufpath.as_str()).exists() {
        println!("cargo:rerun-if-changed=src/flatbuf.rs");
        println!("cargo:rerun-if-changed=src/flatbuf_generated.rs");
    }
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/main.rs");
    println!("cargo:rerun-if-changed=src/api/mod.rs");
    println!("cargo:rerun-if-changed=src/api/c_api.rs");
    println!("cargo:rerun-if-changed=src/ovsdb_api.rs");
    println!("cargo:rerun-if-changed=src/update_handler.rs");

    let lib = "libdatalog_example_ddlog";

    /* Start: fixup for a bug in libtool, which does not correctly
     * remove the symlink it creates.  Remove this fixup once an updated
     * libtool crate is available.
     *
     * See: https://github.com/kanru/libtool-rs/issues/2#issue-440212008
     */
    let topdir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let profile = env::var("PROFILE").unwrap();
    let target_dir = format!("{}/target/{}", topdir, profile);
    let libs_dir = format!("{}/.libs", target_dir);
    let new_lib_path = PathBuf::from(format!("{}/{}.a", libs_dir, lib));
    let _ = fs::remove_file(&new_lib_path);
    /* End: fixup */

    libtool::generate_convenience_lib(lib).unwrap();
}
