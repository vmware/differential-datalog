//! Test the generated Datalog program by reading Datalog commands from stdin (i.e., from a pipe or
//! CLI), parsing them with cmd_parser crate, executing commands, and tracking database state in a
//! map.

#![allow(non_snake_case, dead_code)]

//#![feature(alloc_system)]
//extern crate alloc_system;

extern crate datalog_example;
extern crate differential_datalog;
extern crate cmd_parser;
extern crate time;

#[macro_use]
extern crate rustop;

use std::sync::Arc;
use std::sync::Mutex;
use std::process::exit;
use std::io;
use std::io::{Stdout,stdout,stderr};
use std::env;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use datalog_example::*;
use datalog_example::valmap::*;
use differential_datalog::program::*;
use cmd_parser::*;
use time::precise_time_ns;

// uncomment to enable profiling
//extern crate cpuprofiler;
//use cpuprofiler::PROFILER;

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
    let resp = (
        if !is_upd_cmd(&cmd) {
            apply_updates(p, upds)
        } else {
            Ok(())
        }).and(match cmd {
        Command::Start => {
            p.transaction_start()
        },
        Command::Commit => {
            // uncomment to enable profiling
            //PROFILER.lock().unwrap().start("./prof.profile").expect("Couldn't start profiling");
            let res = p.transaction_commit();
            //PROFILER.lock().unwrap().stop().expect("Couldn't stop profiler");
            res
        },
        Command::Rollback => {
            p.transaction_rollback()
        },
        Command::Timestamp => {
            println!("Timestamp: {}", precise_time_ns());
            Ok(())
        },
        Command::Profile => {
            let profile = (*p.profile.lock().unwrap()).clone();
            println!("Profile:\n{}", profile);
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
                apply_updates(p, upds)
            } else {
                Ok(())
            }
        }
    });
    match resp {
        Ok(_)  => true,
        Err(e) => {eprintln!("Error: {}", e); false}
    }
}

fn apply_updates(p: &mut RunningProgram<Value>, upds: &mut Vec<Update<Value>>) -> Response<()> {
    let copy: Vec<Update<Value>> = upds.drain(..).collect();
    if copy.len() != 0 {
        p.apply_updates(copy)
    } else { Ok(()) }
}

fn is_upd_cmd(c: &Command) -> bool {
    match c {
        Command::Update(_,_) => true,
        _                    => false
    }
}

pub fn run_interactive(db: Arc<Mutex<ValMap>>, upd_cb: UpdateCallback<Value>, nworkers: usize) -> i32 {
    let p = prog(upd_cb);
    let running = Arc::new(Mutex::new(p.run(nworkers)));
    let upds = Arc::new(Mutex::new(Vec::new()));
    interact(|cmd| handle_cmd(&db.clone(), &mut running.lock().unwrap(), &mut upds.lock().unwrap(), cmd))
}

pub fn main() {
    let parser = opts! {
        synopsis "DDlog CLI interface.";
        auto_shorts false;
        opt store:bool=true, desc:"Do not store relation state (for benchmarking only)."; // --no-store
        opt print:bool=true, desc:"Do not print deltas.";                                 // --no-print
        opt workers:usize=4, short:'w', desc:"The number of worker threads.";             // --workers or -w
    };
    let (args, rest) = parser.parse_or_exit();

    if rest.len() != 0 || args.workers == 0 {
        panic!("Invalid command line arguments; try -h for help");
    }

    let print   = args.print;
    let store   = args.store;
    let workers = args.workers;

    let db: Arc<Mutex<ValMap>> = Arc::new(Mutex::new(ValMap::new()));

    let ret = run_interactive(db.clone(), Arc::new(move |relid,v,pol| upd_cb(print,store,&db,relid,v,pol)), workers);
    exit(ret);
}
