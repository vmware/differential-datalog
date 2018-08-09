//! This program helps debug the auto-generated FFI interface to Datalog.  It parses a .dat file
//! using cmd_parser and converts it to a C program that issues an equivalent sequence of Datalog
//! command using the FFI interface.

extern crate datalog_example;
extern crate differential_datalog;
extern crate cmd_parser;

use std::sync::{Arc, Mutex};
use std::process::exit;

use datalog_example::*;
use datalog_example::ffi::*;
use differential_datalog::program::*;
use cmd_parser::*;

static HEADER: &'static str = r###"
#include "datalog_example.h"
#include <stdio.h>
#include <stdbool.h>

void upd_cb(uintptr_t ctx, RelId relid, struct Value* val, bool pol) {
    ValMap vmap = (ValMap) ctx;
    val_map_update(vmap, relid, val, pol);
    val_free(val);
};

int main () {
    int ret;
    ValMap vmap = val_map_new();

    RunningProgram* prog = datalog_example_run(upd_cb, (uintptr_t) vmap);
    if (prog == NULL) return -1;

"###;

static FOOTER: &'static str = r###"
}
"###;

fn updcmd2upd(c: &UpdCmd) -> Result<Update<Value>, String> {
    match c {
        UpdCmd::Insert(rname, rec) => {
            let relid: Relations = relname2id(rname).ok_or(format!("Unknown relation {}", rname))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::Insert{relid: relid as RelId, v: val})
        },
        UpdCmd::Delete(rname, rec) => {
            let relid: Relations = relname2id(rname).ok_or(format!("Unknown relation {}", rname))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::Delete{relid: relid as RelId, v: val})
        }
    }
}

fn handle_cmd(upds: &mut Vec<Update<Value>>, cmd: Command) -> bool {
    match cmd {
        Command::Start => {
            println!("ret = datalog_example_transaction_start(prog);");
        },
        Command::Commit => {
            println!("ret = datalog_example_transaction_commit(prog);");
        },
        Command::Rollback => {
            println!("ret = datalog_example_transaction_rollback(prog);");
        },
        Command::Timestamp => {},
        Command::Dump(None) => {
            println!("val_map_print(vmap);");
        },
        Command::Dump(Some(rname)) => {
            let relid = match relname2id(&rname) {
                None      => {
                    eprintln!("Error: Unknown relation {}", rname);
                    return false;
                },
                Some(rid) => rid as RelId
            };
            println!("val_map_print_rel(vmap, {});", relid);
        },
        Command::Exit => {
            println!("{}", FOOTER);
            exit(0);
        },
        Command::Echo(txt) => {
            println!("printf(\"%s\\n\", {:?}); fflush(stdout);", txt);
        },
        Command::Update(upd, last) => {
             match updcmd2upd(&upd) {
                Ok(u)  => upds.push(u),
                Err(e) => {
                    upds.clear();
                    eprintln!("Error: {}", e);
                    return false;
                }
            };
            if last {
                let copy: Vec<Update<Value>> = upds.drain(..).collect();
                println!("struct Update updates [] = {{");
                for upd in &copy {
                    printUpd(upd);
                }
                println!("}};");
                println!("ret = datalog_example_apply_updates(prog, updates, {});", copy.len());
            };
        }
    };
    true
}

fn printUpd(upd: &Update<Value>) {
    let pol = match upd {
        Update::Insert{..} => "true",
        Update::Delete{..} => "false"
    };
    let v = match upd {
        Update::Insert{relid: _, v} => val_to_ccode(upd.relid(), v).unwrap(),
        Update::Delete{relid: _, v} => val_to_ccode(upd.relid(), v).unwrap()
    };
    println!("    (struct Update){{ .pol={}, .v={} }},", pol, v);
}

pub fn run_interactive() -> i32 {
    println!("{}", HEADER);
    let upds = Arc::new(Mutex::new(Vec::new()));
    interact(|cmd| handle_cmd(&mut upds.lock().unwrap(), cmd))
}

pub fn main() {
    let ret = run_interactive();
    exit(ret);
}
