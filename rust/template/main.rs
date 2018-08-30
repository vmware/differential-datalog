//! Test the generated Datalog program by reading Datalog commands from stdin (i.e., from a pipe or
//! CLI), parsing them with cmd_parser crate, executing commands, and tracking database state in a
//! map.

#![allow(non_snake_case, dead_code)]

extern crate datalog_example;
extern crate differential_datalog;
extern crate cmd_parser;
extern crate time;

use std::sync::Arc;
use std::sync::Mutex;
use std::process::exit;
use std::io;
use std::io::{Stdout,stdout};
use std::env;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use datalog_example::*;
use datalog_example::valmap::*;
use differential_datalog::program::*;
use cmd_parser::*;
use time::precise_time_ns;

fn upd_cb(do_print: bool, do_store: bool, db: &Arc<Mutex<ValMap>>, relid: RelId, v: &Value, pol: bool) {
    if do_store {
        db.lock().unwrap().update(relid, v, pol);
    };
    if do_print {
        eprintln!("{} {:?} {:?}", if pol { "insert" } else { "delete" }, relid, *v);
    };
}

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

fn handle_cmd(db: &Arc<Mutex<ValMap>>, p: &mut RunningProgram<Value>, upds: &mut Vec<Update<Value>>, cmd: Command) -> bool {
    let resp = match cmd {
        Command::Start => {
            upds.clear();
            p.transaction_start()
        },
        Command::Commit => {
            upds.clear();
            p.transaction_commit()
        },
        Command::Rollback => {
            upds.clear();
            p.transaction_rollback()
        },
        Command::Timestamp => {
            println!("Timestamp: {}", precise_time_ns());
            Ok(())
        },
        Command::Dump(None) => {
            db.lock().unwrap().format(&mut stdout());
            Ok(())
        },
        Command::Dump(Some(rname)) => {
            let relid = match relname2id(&rname) {
                None      => {
                    eprintln!("Error: Unknown relation {}", rname);
                    return false;
                },
                Some(rid) => rid as RelId
            };
            db.lock().unwrap().format_rel(relid, &mut stdout());
            Ok(())
        },
        Command::Exit => {
            exit(0);
        },
        Command::Echo(txt) => {
            println!("{}", txt);
            Ok(())
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
                let copy = upds.drain(..).collect();
                p.apply_updates(copy)
            } else {
                Ok(())
            }
        }
    };
    match resp {
        Ok(_)  => true,
        Err(e) => {eprintln!("Error: {}", e); false}
    }
}

pub fn run_interactive(db: Arc<Mutex<ValMap>>, upd_cb: UpdateCallback<Value>) -> i32 {
    let p = prog(upd_cb);
    let running = Arc::new(Mutex::new(p.run(1)));
    let upds = Arc::new(Mutex::new(Vec::new()));
    interact(|cmd| handle_cmd(&db.clone(), &mut running.lock().unwrap(), &mut upds.lock().unwrap(), cmd))
}

pub fn main() {
    let do_store = env::args().find(|a| a.as_str() == "--no-store") == None;
    let do_print = env::args().find(|a| a.as_str() == "--no-print") == None;

    let db: Arc<Mutex<ValMap>> = Arc::new(Mutex::new(ValMap::new()));

    let ret = run_interactive(db.clone(), Arc::new(move |relid,v,pol| upd_cb(do_print,do_store,&db,relid,v,pol)));
    exit(ret);
}
