use std::path::PathBuf;
use std::sync::Mutex;
use std::thread::park;

use env_logger::init;
use once_cell::sync::Lazy;
use log::debug;
use log::info;
use log::log_enabled;
use log::Level::Info;
use serde_json::to_string as to_json;
use structopt::StructOpt;

use distributed_datalog::instantiate;
use distributed_datalog::simple_assign;
use distributed_datalog::zookeeper::connect;
use distributed_datalog::zookeeper::renew_watch;
use distributed_datalog::zookeeper::WatchedEvent;
use distributed_datalog::zookeeper::ZooKeeper;
use distributed_datalog::Addr;
use distributed_datalog::Member;
use distributed_datalog::Members;
use distributed_datalog::ReadConfig;
use distributed_datalog::ReadMembers;
use distributed_datalog::Realization;

use server_api_ddlog::api::HDDlog;
use server_api_ddlog::{DDlogConverter, UpdateSerializer};
use server_api_test::config;

#[derive(StructOpt, Debug)]
#[structopt(name = "server_api")]
enum Opts {
    /// Manually provide required inputs to the configuration.
    Manual {
        /// The address of the current member being instantiated.
        ///
        /// This address needs to be contained in `members`.
        #[structopt(long)]
        member: Addr,

        /// The addresses of members available to the computation.
        #[structopt(long)]
        members: Vec<Addr>,

        /// The input file containing data to feed to the first computation
        /// node.
        #[structopt(long, default_value = "input1.dat", parse(from_os_str))]
        input1: PathBuf,

        /// The input file containing data to feed to the second computation
        /// node.
        #[structopt(long, default_value = "input2.dat", parse(from_os_str))]
        input2: PathBuf,

        /// The path to the output file to dump the results of the computation
        /// into.
        #[structopt(long, default_value = "output.dump", parse(from_os_str))]
        output: PathBuf,
    },

    /// Read the configuration from a ZooKeeper instance.
    #[structopt(name = "zookeeper")]
    ZooKeeper {
        /// The address of the current member being instantiated.
        #[structopt(long)]
        member: Addr,

        /// The addresses of the nodes making up the ZooKeeper cluster.
        #[structopt(long)]
        nodes: Vec<String>,
    },
}

fn manual(
    member: Addr,
    members: Vec<Addr>,
    input1: PathBuf,
    input2: PathBuf,
    output: PathBuf,
) -> Result<(), String> {
    if members.iter().find(|m| *m == &member).is_none() {
        let members = members
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "member {} not found in members ({})",
            member, members
        ));
    }

    let members = members.into_iter().map(Member::new).collect::<Members>();
    if log_enabled!(Info) {
        let members =
            to_json(&members).map_err(|e| format!("failed to serialize members: {}", e))?;
        info!("Using members:\n{}", members);
    }

    let sys_cfg = config(&input1, &input2, &output);
    if log_enabled!(Info) {
        let sys_cfg =
            to_json(&sys_cfg).map_err(|e| format!("failed to serialize configuration: {}", e))?;
        info!("Using configuration:\n{}", sys_cfg);
    }

    let assignment = simple_assign(sys_cfg.keys(), members.iter())
        .ok_or_else(|| format!("failed to find an node:member assignment"))?;
    let _realization = instantiate::<HDDlog, DDlogConverter, UpdateSerializer>(sys_cfg.clone(), &member, &assignment, HDDlog::run(1, false).unwrap().0)?;

    println!("Configuration is running. Stop with Ctrl-C.");
    park();
    Ok(())
}

fn reconfigure(member: &Addr, zookeeper: &ZooKeeper) -> Result<Vec<Realization<DDlogConverter, UpdateSerializer>>, String> {
    let members = zookeeper.members()?;
    let sys_cfg = zookeeper.config()?;

    let assignment = simple_assign(sys_cfg.keys(), members.iter())
        .ok_or_else(|| format!("failed to find an node:member assignment"))?;

    let realization = instantiate::<HDDlog, DDlogConverter, UpdateSerializer>(sys_cfg, member, &assignment, HDDlog::run(1, false).unwrap().0)?;
    Ok(realization)
}

fn reconfig_state(member: &Addr, state: &Mutex<State>) -> Result<(), String> {
    info!("Reconfiguring...");
    let mut guard = state.lock().unwrap();
    // Tear down the old realization before re-instantiating
    // everything.
    drop(guard.realization.take());

    if let Some(zookeeper) = &mut guard.zookeeper {
        // It is not clear why, but we seemingly need to "renew" the
        // watch we already have...
        renew_watch(zookeeper)?;

        match reconfigure(&member, zookeeper) {
            Ok(new) => {
                guard.realization.replace(new);
                Ok(())
            }
            Err(e) => Err(format!("failed to reconfigure: {}", e)),
        }
    } else {
        Ok(())
    }
}

#[derive(Default)]
struct State {
    zookeeper: Option<ZooKeeper>,
    realization: Option<Vec<Realization<DDlogConverter, UpdateSerializer>>>,
}

impl State {
    const fn new() -> Self {
        Self {
            zookeeper: None,
            realization: None,
        }
    }
}

fn zookeeper(member: Addr, nodes: Vec<String>) -> Result<(), String> {
    static STATE: Lazy<Mutex<State>> = Lazy::new(|| Mutex::new(State::new()));

    let watch = move |event: WatchedEvent| {
        debug!("Watch event: {:?}", event);
        if event.path.is_some() {
            let _ = reconfig_state(&member, &STATE);
        }
    };
    let zookeeper = connect(nodes.iter(), watch)?;
    STATE.lock().unwrap().zookeeper = Some(zookeeper);

    reconfig_state(&member, &STATE)?;
    println!("Configuration is running. Stop with Ctrl-C.");
    park();
    Ok(())
}

fn main() -> Result<(), String> {
    init();

    let opts = Opts::from_args();
    match opts {
        Opts::Manual {
            member,
            members,
            input1,
            input2,
            output,
        } => manual(member, members, input1, input2, output),
        Opts::ZooKeeper { member, nodes } => zookeeper(member, nodes),
    }
}
