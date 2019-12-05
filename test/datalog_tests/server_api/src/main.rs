use std::path::PathBuf;
use std::thread::park;

use log::info;
use log::log_enabled;
use log::Level::Info;
use serde_json::to_string as to_json;
use structopt::StructOpt;

use distributed_datalog::instantiate;
use distributed_datalog::simple_assign;
use distributed_datalog::Addr;
use distributed_datalog::Member;
use distributed_datalog::Members;

use server_api_ddlog::api::HDDlog;
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
    let _realization = instantiate::<HDDlog>(sys_cfg.clone(), &member, &assignment)?;

    println!("Configuration is running. Stop with Ctrl-C.");
    park();
    Ok(())
}

fn main() -> Result<(), String> {
    let opts = Opts::from_args();
    match opts {
        Opts::Manual {
            member,
            members,
            input1,
            input2,
            output,
        } => manual(member, members, input1, input2, output),
    }
}
