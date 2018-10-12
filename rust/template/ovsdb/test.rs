use super::*;

use std::{io,env};
use std::io::BufRead;
use std::fs::{self, DirEntry};
use std::path::Path;

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
        if let Value::Object(json_val) = serde_json::from_str(&line.unwrap()).unwrap() {
            let cmds = cmds_from_table_updates("", json_val).unwrap();
            //println!("parsed commands:\n{:?}", cmds);
        } else {
            panic!("parse_json_file: JSON value is not an object")
        }
    }
}
