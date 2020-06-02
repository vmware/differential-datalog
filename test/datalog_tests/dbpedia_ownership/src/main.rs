use std::path::Path;
use std::path::PathBuf;
use std::thread::park;

use env_logger::init;
use maplit::btreemap;
use maplit::btreeset;
use structopt::StructOpt;
use uuid::Uuid;

use distributed_datalog::instantiate;
use distributed_datalog::simple_assign;
use distributed_datalog::Addr;
use distributed_datalog::Member;
use distributed_datalog::Members;
use distributed_datalog::RelCfg;
use distributed_datalog::Sink;
use distributed_datalog::Source;
use distributed_datalog::SysCfg;

use dbpedia_ownership_ddlog::api::HDDlog;
use dbpedia_ownership_ddlog::Relations::Ownership;
use dbpedia_ownership_ddlog::Relations::Subsidiaries;

#[derive(StructOpt, Debug)]
#[structopt(name = "dbpedia_ownership")]
enum Opts {
    /// Instantiate the computation.
    #[structopt(name = "run")]
    Run {
        /// The address of the current member being instantiated.
        #[structopt(long)]
        member: Addr,
        /// The addresses of members available to the computation.
        #[structopt(long)]
        members: Vec<Addr>,
        /// The path to the input file containing `Subsidiaries` transactions.
        #[structopt(
            long = "subsidiaries",
            default_value = "data/subsidiaries.ddlog.dat",
            parse(from_os_str)
        )]
        #[structopt(long = "subsidiaries", parse(from_os_str))]
        subsidiaries_data: PathBuf,
        /// The path to the output file containing `Ownership` transactions.
        #[structopt(
            long = "ownership",
            default_value = "ownership.dump",
            parse(from_os_str)
        )]
        ownership_data: PathBuf,
    },
}

/// Retrieve the system configuration.
fn config(subsidiaries_data: &Path, ownership_data: &Path) -> SysCfg {
    // We require UUIDs in ascending order to be able to create a stable
    // assignment.
    let node_id = Uuid::parse_str("7aba4dd8-80fc-43f0-bbf6-0dc56abaa001").unwrap();

    let node_cfg = btreemap! {
        Subsidiaries as usize => btreeset!{
            RelCfg::Source(Source::File(subsidiaries_data.to_path_buf())),
        },
        Ownership as usize => btreeset!{
            RelCfg::Sink(Sink::File(ownership_data.to_path_buf())),
        }
    };

    btreemap! {
        node_id => node_cfg,
    }
}

fn realize(
    member: Addr,
    members: Vec<Addr>,
    subsidiaries_data: &Path,
    ownership_data: &Path,
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
    let sys_cfg = config(subsidiaries_data, ownership_data);
    let assignment = simple_assign(sys_cfg.keys(), members.iter())
        .ok_or_else(|| format!("failed to find an node:member assignment"))?;
    let _realization = instantiate::<HDDlog>(sys_cfg.clone(), &member, &assignment)?;

    println!("Configuration is running. Stop with Ctrl-C.");
    park();
    Ok(())
}

fn main() -> Result<(), String> {
    init();

    let opts = Opts::from_args();
    match opts {
        Opts::Run {
            member,
            members,
            subsidiaries_data,
            ownership_data,
        } => realize(member, members, &subsidiaries_data, &ownership_data),
    }
}
