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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use datalog_example::*;
use differential_datalog::program::*;
use cmd_parser::*;
use time::precise_time_ns;

pub type ValMap = BTreeMap<RelId, BTreeSet<Value>>;

fn valMapFormat(vmap: &ValMap, w: &mut io::Write) {
   for (relid, relset) in vmap {
       w.write_fmt(format_args!("{:?}:\n", relid2rel(*relid).unwrap()));
       for val in relset {
            w.write_fmt(format_args!("{}\n", *val));
       };
       w.write_fmt(format_args!("\n"));
   };
}

fn valMapGetRel(vmap: &mut ValMap, relid: RelId) -> &BTreeSet<Value> {
    vmap.entry(relid).or_insert(BTreeSet::default())
}

fn upd_cb(db: &Arc<Mutex<ValMap>>, relid: RelId, v: &Value, pol: bool) {
    set_update(relid, db, v, pol);
    eprintln!("{} {:?} {:?}", if pol { "insert" } else { "delete" }, relid, *v);
}

fn set_update(relid: RelId, s: &Arc<Mutex<ValMap>>, x : &Value, insert: bool)
{
    //println!("set_update({}) {:?} {}", rel, *x, insert);
    if insert {
        s.lock().unwrap().entry(relid).or_insert(BTreeSet::default()).insert(x.clone());
    } else {
        s.lock().unwrap().entry(relid).or_insert(BTreeSet::default()).remove(x);
    }
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
            valMapFormat(&*db.lock().unwrap(), &mut stdout());
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
            let mut db = db.lock().unwrap();
            let set = valMapGetRel(&mut *db, relid);
            for val in set {
                println!("{}", *val);
            };
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
    let db: Arc<Mutex<ValMap>> = Arc::new(Mutex::new(BTreeMap::default()));

    let ret = run_interactive(db.clone(), Arc::new(move |relid,v,pol| upd_cb(&db,relid,v,pol)));
    exit(ret);
}
