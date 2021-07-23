//! Test the generated Datalog program by reading Datalog commands from stdin (i.e., from a pipe or
//! CLI), parsing them with cmd_parser crate, executing commands, and tracking database state in a
//! map.

#![allow(dead_code, non_snake_case, clippy::match_like_matches_macro)]

use std::{
    convert::TryFrom,
    io::{stdout, Write},
    net::SocketAddr,
    process,
    process::Stdio,
    str::FromStr,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use cmd_parser::*;
use datalog_example_ddlog::*;
use ddlog_log::log_set_default_callback;
use differential_datalog::{
    api::HDDlog,
    ddval::*,
    program::config::{Config, LoggingDestination, ProfilingConfig},
    program::*,
    record::*,
    DDlog, DDlogDynamic, DDlogProfiling, DeltaMap,
};
use num_traits::cast::ToPrimitive;
use rustop::opts;

#[cfg(feature = "profile")]
use cpuprofiler::PROFILER;

#[cfg(feature = "distribution")]
mod d3main;

const DDSHOW_TIMEOUT_MILLIS: u64 = 3_000;

#[allow(clippy::let_and_return)]
fn handle_cmd(
    start_time: Instant,
    hddlog: &HDDlog,
    print_deltas: bool,
    interactive: bool,
    upds: &mut Vec<Update<DDValue>>,
    cmd: Command,
) -> (Result<(), String>, bool) {
    let resp = (if !is_upd_cmd(&cmd) {
        apply_updates(hddlog, upds)
    } else {
        Ok(())
    })
    .and(match cmd {
        Command::Start => hddlog.transaction_start(),
        Command::LogLevel(level) => {
            log_set_default_callback(
                Some(Box::new(move |level, msg| {
                    println!("LOG ({}): {}", level, msg);
                })),
                level,
            );
            Ok(())
        }
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
                        dump_delta(&changes)
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
            println!("Timestamp: {}", start_time.elapsed().as_nanos());
            Ok(())
        }
        Command::Profile(None) => hddlog
            .profile()
            .map(|profile| println!("Profile:\n{}", profile)),
        Command::Profile(Some(ProfileCmd::Cpu(enable))) => hddlog.enable_cpu_profiling(enable),
        Command::Profile(Some(ProfileCmd::Timely(enable))) => {
            hddlog.enable_timely_profiling(enable)
        }

        Command::Dump(None) => {
            let _ = hddlog
                .db
                .as_ref()
                .map(|db| db.lock().unwrap().format_as_sets(&mut stdout(), &Inventory));

            Ok(())
        }
        Command::Dump(Some(rname)) => {
            let relid = match Relations::try_from(rname.as_str()) {
                Ok(rid) if rid.is_output() => rid as RelId,
                _ => {
                    let err = format!("Unknown output relation {}", rname);
                    if interactive {
                        eprintln!("Error: {}", err);
                    }
                    return (Err(err), interactive);
                }
            };
            let _ = hddlog
                .db
                .as_ref()
                .map(|db| db.lock().unwrap().format_rel_as_set(relid, &mut stdout()));
            Ok(())
        }
        Command::Clear(rname) => {
            let relid = match Relations::try_from(rname.as_str()) {
                Ok(rid) if rid.is_input() => rid as RelId,
                _ => {
                    let err = format!("Unknown input relation {}", rname);
                    if interactive {
                        eprintln!("Error: {}", err);
                    }
                    return (Err(err), interactive);
                }
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
            thread::sleep(std::time::Duration::from_millis(ms.to_u64().unwrap()));
            Ok(())
        }
        Command::Update(update, last) => {
            match hddlog.convert_update_command(&update) {
                Ok(update) => upds.push(update),
                Err(err) => {
                    upds.clear();
                    if interactive {
                        eprintln!("Error: {}", err);
                    }

                    return (Err(err), interactive);
                }
            }

            if last {
                apply_updates(hddlog, upds)
            } else {
                Ok(())
            }
        }
        Command::QueryIndex(idx, key) => Indexes::try_from(idx.as_str())
            .map_err(|_| format!("Unknown index {}", idx))
            .and_then(|idxid| {
                idxkey_from_record(idxid, &key)
                    .and_then(|keyval| hddlog.query_index(idxid as IdxId, keyval))
            })
            .map(|vals| {
                for val in vals.into_iter() {
                    let _ = writeln!(stdout(), "{}", val.clone().into_record());
                }
            }),
        Command::DumpIndex(idx) => Indexes::try_from(idx.as_str())
            .map_err(|_| format!("Unknown index {}", idx))
            .and_then(|idxid| hddlog.dump_index(idxid as IdxId))
            .map(|vals| {
                for val in vals.into_iter() {
                    let _ = writeln!(stdout(), "{}", val.clone().into_record());
                }
            }),
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

fn dump_delta(delta: &DeltaMap<DDValue>) {
    for (table_id, table_data) in delta.iter() {
        let _ = writeln!(stdout(), "{}:", relid2name(*table_id).unwrap());
        for (val, weight) in table_data.iter() {
            //debug_assert!(*weight == 1 || *weight == -1);
            let _ = writeln!(stdout(), "{}: {:+}", val.clone().into_record(), *weight);
        }
    }
}

fn apply_updates(hddlog: &HDDlog, upds: &mut Vec<Update<DDValue>>) -> Response<()> {
    if !upds.is_empty() {
        hddlog.apply_updates(&mut upds.drain(..))
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

fn run(hddlog: HDDlog, print_deltas: bool) -> Result<(), String> {
    let upds = Arc::new(Mutex::new(Vec::new()));
    let start_time = Instant::now();
    interact(|cmd, interactive| {
        handle_cmd(
            start_time,
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
        opt store:bool=true, desc:"Do not store output relation state. 'dump' and 'dump <table>' commands will produce no output."; // --no-store
        opt delta:bool=true, desc:"Do not record changes. 'commit dump_changes' will produce no output.";                           // --no-delta
        opt init_snapshot:bool=true, desc:"Do not dump initial output snapshot.";                                                   // --no-init-snapshot
        opt print:bool=true, desc:"Backwards compatibility. The value of this flag is ignored.";                                    // --no-print
        opt workers:usize=1, short:'w', desc:"The number of worker threads. Default is 1.";                                         // --workers or -w
        opt idle_merge_effort:Option<isize>, desc:"Set Differential Dataflow's 'idle_merge_effort' parameter. This flag takes precedence over the '$DIFFERENTIAL_EAGER_MERGE' environment variable.";
        opt profile_timely:bool=false, desc:"Use external Timely Dataflow profiler.";
        opt profile_differential:bool=false, desc:"Use external Differential Dataflow profiler. Implies '--profile-timely'";
        opt self_profiler:bool, desc:"Enable DDlog internal profiler. This option is mutually exclusive with '--profile-timely'.";
        opt timely_profiler_socket:Option<String>, desc:"Socket address to send Timely Dataflow profiling events. Default (if '--profile-timely' is specified) is '127.0.0.1:51317'. Implies '--profile-timely'.";
        opt timely_trace_dir:Option<String>, desc:"Path to a directory to store Timely Dataflow profiling events, e.g., './timely_trace'. Implies '--profile-timely'.";
        opt differential_profiler_socket:Option<String>, desc:"Socket address to send Differential Dataflow profiling events. Default (if '--profile-differential' is specified is '127.0.0.1:51318'. Implies '--profile-differential'.";
        opt differential_trace_dir:Option<String>, desc:"Path to a directory to store Differential Dataflow profiling events, e.g., './differential_trace'. Implies '--profile-differential'.";
        opt ddshow:bool=false, desc:"Start 'ddshow' profiler on sockets specified by '--timely-profiler-socket' and (optionally) '--differential-profiler-socket' options. Implies '--timely-profiler'.";
    };
    let (mut args, rest) = parser.parse_or_exit();

    if !rest.is_empty() {
        return Err("Invalid command line arguments; try -h for help".to_string());
    }

    let mut config = Config {
        num_timely_workers: if args.workers == 0 {
            return Err("Invalid number of workers: 0".to_string());
        } else {
            args.workers
        },
        differential_idle_merge_effort: args.idle_merge_effort,
        ..Default::default()
    };

    // Propagate implied options.

    // 'differential-profiler-socket', 'differential-trace-dir' imply 'profile-differential'.
    if args.differential_profiler_socket.is_some() || args.differential_trace_dir.is_some() {
        args.profile_differential = true;
    }

    // 'profile-differential', 'timely-profiler-socket', 'timely-trace-dir', 'ddshow' imply 'profile-timely'.
    if args.profile_differential
        || args.timely_profiler_socket.is_some()
        || args.timely_trace_dir.is_some()
        || args.ddshow
    {
        args.profile_timely = true;
    }

    // If 'profile-timely' is set, and neither 'timely-profiler-socket' nor 'timely-trace-dir' are
    // specified, 'timely-profiler-socket' defaults to "127.0.0.1:51317".
    if args.profile_timely
        && (args.timely_profiler_socket.is_none() && args.timely_trace_dir.is_none())
    {
        args.timely_profiler_socket = Some("127.0.0.1:51317".to_string());
    }

    // If 'profile-differential' is set, and neither 'differential-profiler-socket' nor 'differential-trace-dir' are
    // specified, 'differential-profiler-socket' defaults to "127.0.0.1:51318".
    if args.profile_differential
        && (args.differential_profiler_socket.is_none() && args.differential_trace_dir.is_none())
    {
        args.differential_profiler_socket = Some("127.0.0.1:51318".to_string());
    }

    // Check for conflicts

    // 'differential-profiler-socket' and 'differential-trace-dir' are mutually exclusive.
    if args.differential_profiler_socket.is_some() && args.differential_trace_dir.is_some() {
        return Err(
            "Conflicting options: --differential-profiler-socket and --differential-trace-dir."
                .to_string(),
        );
    }

    // 'timely-profiler-socket' and 'timely-trace-dir' are mutually exclusive.
    if args.timely_profiler_socket.is_some() && args.timely_trace_dir.is_some() {
        return Err(
            "Conflicting options: --timely-profiler-socket and --timely-trace-dir.".to_string(),
        );
    }

    // 'self-profiler' and 'profile-timely' are mutually exclusive.
    if args.self_profiler && args.profile_timely {
        return Err("Conflicting options: --self-profiler and --profile-timely.".to_string());
    }

    // 'ddshow' and 'timely-trace-dir' are mutually exclusive.
    if args.ddshow && args.timely_trace_dir.is_some() {
        return Err(
            "--ddshow conflicts with --timely-trace-dir. Use --timely-profiler-socket instead."
                .to_string(),
        );
    }

    // 'differential-profiler-socket' requires 'timely-profiler-socket'
    if args.differential_profiler_socket.is_some() && args.timely_profiler_socket.is_none() {
        return Err(
            "--differential-profiler-socket requires --timely-profiler-socket.".to_string(),
        );
    }

    // 'differential-trace-dir' requires 'timely-trace-dir'
    if args.differential_trace_dir.is_some() && args.timely_trace_dir.is_none() {
        return Err("--differential-trace-dir requires --timely-trace-dir.".to_string());
    }

    let timely_socket = if let Some(sockaddr) = args.timely_profiler_socket {
        Some(
            SocketAddr::from_str(&sockaddr)
                .map_err(|e| format!("Invalid socket address '{}': {}", sockaddr, e))?,
        )
    } else {
        None
    };

    let differential_socket = if let Some(sockaddr) = args.differential_profiler_socket {
        Some(
            SocketAddr::from_str(&sockaddr)
                .map_err(|e| format!("Invalid socket address '{}': {}", sockaddr, e))?,
        )
    } else {
        None
    };

    config.profiling_config = match (
        &args.self_profiler,
        &args.profile_timely,
        &args.profile_differential,
    ) {
        (false, false, false) => ProfilingConfig::None,
        (true, _, _) => ProfilingConfig::SelfProfiling,
        _ => ProfilingConfig::TimelyProfiling {
            timely_destination: {
                if let Some(sockaddr) = timely_socket {
                    LoggingDestination::Socket { sockaddr }
                } else {
                    LoggingDestination::Disk {
                        directory: args.timely_trace_dir.unwrap(),
                    }
                }
            },
            timely_progress_destination: None,
            differential_destination: {
                if let Some(sockaddr) = differential_socket {
                    Some(LoggingDestination::Socket { sockaddr })
                } else {
                    args.differential_trace_dir
                        .map(|directory| LoggingDestination::Disk { directory })
                }
            },
        },
    };

    let ddshow = if args.ddshow {
        Some(start_ddshow(
            &timely_socket.unwrap(),
            &differential_socket,
            config.num_timely_workers,
        )?)
    } else {
        None
    };

    let ddlog_res = match crate::run_with_config(config, args.store) {
        Ok((hddlog, init_output)) => {
            if args.init_snapshot {
                dump_delta(&init_output);
            }
            run(hddlog, args.delta)
        }
        Err(err) => Err(format!("Failed to run differential datalog: {}", err)),
    };

    // Give ddshow a chance to wrap up.
    if let Some(mut ddshow_process) = ddshow {
        if ddlog_res.is_err() {
            // Kill ddshow, as it may not be able to shut down cleanly at this point.
            // Ignore error that will occur if 'ddshow' has already terminated.
            let _ = ddshow_process.kill();
        } else {
            eprintln!("Waiting for ddshow");
            // Use `try_wait` instead of `wait`, which closes ddshow's stdin, causing it to die abnormally.
            loop {
                if ddshow_process
                    .try_wait()
                    .map_err(|e| format!("Error waiting for the ddshow process: {} ", e))?
                    .is_some()
                {
                    break;
                };
            }
        }
    };

    //    #[cfg(feature = "distribution")]
    //    {
    //        match d3main::start_d3log() {
    //            Ok(x) => Ok(x),
    //            Err(x) => Err(x.to_string()),
    //        }
    //    }

    ddlog_res
}

fn start_ddshow(
    timely_socket: &SocketAddr,
    differential_socket: &Option<SocketAddr>,
    nworkers: usize,
) -> Result<process::Child, String> {
    let mut cmd = process::Command::new("ddshow");
    cmd.args(&[
        "--connections",
        &nworkers.to_string(),
        "--workers",
        &nworkers.to_string(),
        "--address",
        &timely_socket.to_string(),
        "--stream-encoding",
        "rkyv",
        "--disable-timeline",
    ]);
    if let Some(sockaddr) = differential_socket {
        cmd.args(&[
            "--differential",
            "--differential-address",
            &sockaddr.to_string(),
        ]);
    }
    cmd.stdin(Stdio::piped());

    let child = cmd.spawn().map_err(|e| {
        format!(
            "Failed to start ddshow: {}. Make sure that ddshow is installed and is in $PATH.",
            e
        )
    })?;

    thread::sleep(Duration::from_millis(300));
    Ok(child)
}
