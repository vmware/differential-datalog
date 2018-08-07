//! This program helps debug the auto-generated FFI interface to Datalog.  It parses a .dat file
//! using cmd_parser and converts it to a C program that issues an equivalent sequence of Datalog
//! command using the FFI interface.

extern crate datalog_example;
extern crate differential_datalog;
extern crate cmd_parser;

use std::sync::{Arc, Mutex};
use std::process::exit;

use datalog_example::*;
use differential_datalog::program::*;
use cmd_parser::*;

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
        Command::Dump => {
            panic!("Dump: not implemented");
        },
        Command::Exit => {
            exit(0);
        },
        Command::Echo(txt) => {
            println!("println(\"%s\", {})", txt);
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
                for upd in &copy {
                    printUpd(upd);
                }
                println!("ret = datalog_example_apply_updates(prog, updates, {});", copy.len());
            };
        }
    };
    true
}

fn printUpd(upd: &Update<Value>) {
    panic!("printUpd: not implemented");
}

pub fn run_interactive() -> i32 {
    let upds = Arc::new(Mutex::new(Vec::new()));
    interact(|cmd| handle_cmd(&mut upds.lock().unwrap(), cmd))
}

pub fn main() {
    let ret = run_interactive();
    exit(ret);
}
