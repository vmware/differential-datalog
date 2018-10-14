use super::*;

use std::{io,env};
use std::io::BufRead;
use std::fs::{self, DirEntry};
use std::path::Path;


/* test_data was copied from OVN test directory using:
 * `find ~/Downloads/ovs-reviews/tests/testsuite.dir/ -name '*.rawsync' | cpio -pdm ./test_data/`
 */

#[test]
fn test_parser() {
    let mut test_data_dir = env::current_dir().unwrap();
    test_data_dir.push("test_data");
    visit_dirs(&test_data_dir, &parse_json_file);
}

fn visit_dirs(dir: &Path, cb: &Fn(&DirEntry)) {
    //println!("parsing directory {}", dir.to_str().unwrap());
    if dir.is_dir() {
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb);
            } else {
                cb(&entry);
            }
        }
    }
}

fn parse_json_file(entry: &DirEntry) {
    let path = entry.path().to_str().unwrap().to_owned();
    //println!("parsing file {}", path);
    let f = io::BufReader::new(fs::File::open(path).unwrap());
    for line in f.lines() {
        cmds_from_table_updates_str("", line.unwrap().as_ref()).unwrap();
    }
}
