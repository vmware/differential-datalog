//! Test the generated Datalog program by reading Datalog commands from stdin (i.e., from a pipe or
//! CLI), parsing them with cmd_parser crate, executing commands, and tracking database state in a
//! map.

#![allow(dead_code, non_snake_case)]

use std::io::stdout;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;

use api::{updcmd2upd, HDDlog};
use cmd_parser::*;
use datalog_example_ddlog::*;
use differential_datalog::program::*;
use differential_datalog::record::*;
use num_traits::cast::ToPrimitive;
use rustop::opts;
use time::precise_time_ns;

#[cfg(feature = "profile")]
use cpuprofiler::PROFILER;

#[allow(clippy::let_and_return)]
fn handle_cmd(
    hddlog: &HDDlog,
    print_deltas: bool,
    interactive: bool,
    upds: &mut Vec<Update<Value>>,
    cmd: Command,
) -> (Result<(), String>, bool) {
    let resp = (if !is_upd_cmd(&cmd) {
        apply_updates(hddlog, upds)
    } else {
        Ok(())
    })
    .and(match cmd {
        Command::Start => hddlog.transaction_start(),
        Command::Commit(record_delta) => {
            #[cfg(feature = "profile")]
            {
                PROFILER
                    .lock()
                    .unwrap()
                    .start("./prof.profile")
                    .expect("Couldn't start profiling");
            }

            let res = if record_delta {
                hddlog.transaction_commit_dump_changes().map(|changes| {
                    if print_deltas {
                        for (table_id, table_data) in changes.as_ref().iter() {
                            let _ = writeln!(stdout(), "{}:", relid2name(*table_id).unwrap());
                            for (val, weight) in table_data.iter() {
                                debug_assert!(*weight == 1 || *weight == -1);
                                let _ = writeln!(
                                    stdout(),
                                    "{}: {:+}",
                                    val.clone().into_record(),
                                    *weight
                                );
                            }
                        }
                    }
                })
            } else {
                hddlog.transaction_commit()
            };

            #[cfg(feature = "profile")]
            {
                PROFILER
                    .lock()
                    .unwrap()
                    .stop()
                    .expect("Couldn't stop profiler");
            }
            res
        }
        Command::Comment => Ok(()),
        Command::Rollback => hddlog.transaction_rollback(),
        Command::Timestamp => {
            println!("Timestamp: {}", precise_time_ns());
            Ok(())
        }
        Command::Profile(None) => {
            let profile = hddlog.profile();
            println!("Profile:\n{}", profile);
            Ok(())
        }
        Command::Profile(Some(ProfileCmd::CPU(enable))) => {
            hddlog.enable_cpu_profiling(enable);
            Ok(())
        }
        Command::Dump(None) => {
            let _ = hddlog
                .db
                .as_ref()
                .map(|db| db.lock().unwrap().format_as_sets(&mut stdout()));
            Ok(())
        }
        Command::Dump(Some(rname)) => {
            let relid = match output_relname_to_id(&rname) {
                None => {
                    let err = format!("Unknown output relation {}", rname);
                    if interactive {
                        eprintln!("Error: {}", err);
                    }
                    return (Err(err), interactive);
                }
                Some(rid) => rid as RelId,
            };
            let _ = hddlog
                .db
                .as_ref()
                .map(|db| db.lock().unwrap().format_rel_as_set(relid, &mut stdout()));
            Ok(())
        }
        Command::Clear(rname) => {
            let relid = match input_relname_to_id(&rname) {
                None => {
                    let err = format!("Unknown input relation {}", rname);
                    if interactive {
                        eprintln!("Error: {}", err);
                    }
                    return (Err(err), interactive);
                }
                Some(rid) => rid as RelId,
            };
            hddlog.clear_relation(relid)
        }
        Command::Exit => {
            return (Ok(()), false);
        }
        Command::Echo(txt) => {
            println!("{}", txt);
            Ok(())
        }
        Command::Sleep(ms) => {
            sleep(Duration::from_millis(ms.to_u64().unwrap()));
            Ok(())
        }
        Command::Update(upd, last) => {
            match updcmd2upd(&upd) {
                Ok(u) => upds.push(u),
                Err(e) => {
                    upds.clear();
                    if interactive {
                        eprintln!("Error: {}", e);
                    }
                    return (Err(e), interactive);
                }
            };
            if last {
                apply_updates(hddlog, upds)
            } else {
                Ok(())
            }
        }
    });
    match resp {
        Ok(_) => (Ok(()), true),
        Err(e) => {
            if interactive {
                eprintln!("Error: {}", e);
            }
            (Err(e), interactive)
        }
    }
}

fn apply_updates(hddlog: &HDDlog, upds: &mut Vec<Update<Value>>) -> Response<()> {
    if !upds.is_empty() {
        hddlog.apply_valupdates(upds.drain(..))
    } else {
        Ok(())
    }
}

fn is_upd_cmd(c: &Command) -> bool {
    match c {
        Command::Update(_, _) => true,
        _ => false,
    }
}

fn run(mut hddlog: HDDlog, print_deltas: bool) -> Result<(), String> {
    let upds = Arc::new(Mutex::new(Vec::new()));
    interact(|cmd, interactive| {
        handle_cmd(
            &hddlog,
            print_deltas,
            interactive,
            &mut upds.lock().unwrap(),
            cmd,
        )
    })?;

    hddlog.stop()
}

#[allow(clippy::redundant_closure)]
fn main() -> Result<(), String> {
    let parser = opts! {
        synopsis "DDlog CLI interface.";
        auto_shorts false;
        opt store:bool=true, desc:"Do not store relation state (for benchmarking only)."; // --no-store
        opt delta:bool=true, desc:"Do not record changes.";                               // --no-delta
        opt print:bool=true, desc:"Do not print deltas.";                                 // --no-print
        opt workers:usize=4, short:'w', desc:"The number of worker threads.";             // --workers or -w
    };
    let (args, rest) = parser.parse_or_exit();

    if !rest.is_empty() || args.workers == 0 {
        return Err("Invalid command line arguments; try -h for help".to_string());
    }

    fn record_upd(table: usize, rec: &Record, w: isize) {
        eprintln!(
            "{}({:+}) {:?} {}",
            if w >= 0 { "insert" } else { "delete" },
            w,
            table,
            *rec
        );
    }
    fn no_op(_table: usize, _rec: &Record, _w: isize) {}
    let cb = if args.print { record_upd } else { no_op };

    match HDDlog::run(args.workers, args.store, cb) {
        Ok(hddlog) => run(hddlog, args.delta),
        Err(err) => Err(format!("Failed to run differential datalog: {}", err)),
    }
}
