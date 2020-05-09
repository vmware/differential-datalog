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

use ip_discovery_ddlog::api::HDDlog;
use ip_discovery_ddlog::Relations::ip_discovery_agent_LSPConfig;
use ip_discovery_ddlog::Relations::ip_discovery_agent_RealizedAddress;
use ip_discovery_ddlog::Relations::ip_discovery_agent_SnoopedAddress;
use ip_discovery_ddlog::Relations::ip_discovery_controller_EffectiveAddress;
use ip_discovery_ddlog::Relations::ip_discovery_controller_LSPConfig;
use ip_discovery_ddlog::Relations::ip_discovery_controller_LogicalSwitchPort;
use ip_discovery_ddlog::Relations::ip_discovery_controller_RealizedAddress;

#[derive(StructOpt, Debug)]
#[structopt(name = "ip_discovery")]
enum Opts {
    /// Instantiate the controller.
    #[structopt(name = "ctrl")]
    Ctrl {
        /// The address of the current member being instantiated.
        #[structopt(long)]
        member: Addr,
        /// The addresses of members available to the computation.
        #[structopt(long)]
        members: Vec<Addr>,
        /// The path to the input file containing `LogicalSwitchPort`
        /// transactions.
        #[structopt(long = "lsp", parse(from_os_str))]
        lsp_data: PathBuf,
        /// The path to the output file containing `EffectiveAddress`
        /// transactions.
        #[structopt(
            long = "effective",
            default_value = "effective.dump",
            parse(from_os_str)
        )]
        effective_data: PathBuf,
    },
    /// Instantiate an agent.
    #[structopt(name = "agent")]
    Agent {
        /// The address of the current member being instantiated.
        #[structopt(long)]
        member: Addr,
        /// The addresses of members available to the computation.
        #[structopt(long)]
        members: Vec<Addr>,
        /// The path to the input file containing `SnoopedAddress`
        /// transactions.
        #[structopt(long = "snooped", parse(from_os_str))]
        snooped_data: PathBuf,
        /// The path to the output file containing `RealizedAddress`
        /// transactions.
        #[structopt(long = "realized", default_value = "realized.dump", parse(from_os_str))]
        realized_data: PathBuf,
    },
}

/// Retrieve the system configuration.
fn config(
    lsp_data: &Path,
    snooped_address: &Path,
    effective_address: &Path,
    realized_address: &Path,
) -> SysCfg {
    // We require UUIDs in ascending order to be able to create a stable
    // assignment.
    let agent_id = Uuid::parse_str("7aba4dd8-80fc-43f0-bbf6-0dc56abaa001").unwrap();
    let ctrl_id = Uuid::parse_str("bcda5ea4-b27d-4baf-b405-dfed7cd739ae").unwrap();

    let ctrl_cfg = btreemap! {
        ip_discovery_controller_LogicalSwitchPort as usize => btreeset!{
            RelCfg::Source(Source::File(lsp_data.to_path_buf())),
        },
        ip_discovery_controller_LSPConfig as usize => btreeset!{
            RelCfg::Output(agent_id.clone(), ip_discovery_agent_LSPConfig as usize),
        },
        ip_discovery_controller_EffectiveAddress as usize => btreeset!{
            RelCfg::Sink(Sink::File(effective_address.to_path_buf())),
        },
        ip_discovery_controller_RealizedAddress as usize => btreeset!{
            RelCfg::Input(ip_discovery_agent_RealizedAddress as usize),
        },
    };
    let agent_cfg = btreemap! {
        ip_discovery_agent_LSPConfig as usize => btreeset!{
            RelCfg::Input(ip_discovery_controller_LSPConfig as usize),
        },
        ip_discovery_agent_RealizedAddress as usize => btreeset!{
            RelCfg::Output(ctrl_id.clone(), ip_discovery_controller_RealizedAddress as usize),
            RelCfg::Sink(Sink::File(realized_address.to_path_buf())),
        },
        ip_discovery_agent_SnoopedAddress as usize => btreeset!{
            RelCfg::Source(Source::File(snooped_address.to_path_buf())),
        },
    };

    let sys_cfg = btreemap! {
        ctrl_id => ctrl_cfg,
        agent_id => agent_cfg,
    };

    sys_cfg
}

fn realize(
    member: Addr,
    members: Vec<Addr>,
    lsp_data: &Path,
    snooped_address: &Path,
    effective_address: &Path,
    realized_address: &Path,
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
    let sys_cfg = config(
        lsp_data,
        snooped_address,
        effective_address,
        realized_address,
    );
    let assignment = simple_assign(sys_cfg.keys(), members.iter())
        .ok_or_else(|| format!("failed to find an node:member assignment"))?;
    let _realization = instantiate::<HDDlog>(sys_cfg.clone(), &member, &assignment)?;

    println!("Configuration is running. Stop with Ctrl-C.");
    park();
    Ok(())
}

fn main() -> Result<(), String> {
    init();

    let empty = Path::new("/tmp/whatever");
    let opts = Opts::from_args();
    match opts {
        Opts::Ctrl {
            member,
            members,
            lsp_data,
            effective_data,
        } => realize(member, members, &lsp_data, &empty, &effective_data, &empty),
        Opts::Agent {
            member,
            members,
            snooped_data,
            realized_data,
        } => realize(
            member,
            members,
            &empty,
            &snooped_data,
            &empty,
            &realized_data,
        ),
    }
}
