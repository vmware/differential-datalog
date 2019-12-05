use std::path::Path;

use maplit::btreemap;
use maplit::btreeset;
use uuid::Uuid;

use distributed_datalog::RelCfg;
use distributed_datalog::Sink;
use distributed_datalog::Source;
use distributed_datalog::SysCfg;

use server_api_ddlog::Relations::server_api_1_P1In;
use server_api_ddlog::Relations::server_api_1_P1Out;
use server_api_ddlog::Relations::server_api_2_P2In;
use server_api_ddlog::Relations::server_api_2_P2Out;
use server_api_ddlog::Relations::server_api_3_P1Out;
use server_api_ddlog::Relations::server_api_3_P2Out;
use server_api_ddlog::Relations::server_api_3_P3Out;

/// Retrieve the system configuration.
pub fn config(input1: &Path, input2: &Path, output: &Path) -> SysCfg {
    // We require UUIDs in ascending order to be able to create a stable
    // assignment.
    let uuids = vec![
        Uuid::parse_str("7aba4dd8-80fc-43f0-bbf6-0dc56abaa001").unwrap(),
        Uuid::parse_str("bcda5ea4-b27d-4baf-b405-dfed7cd739ae").unwrap(),
        Uuid::parse_str("c2737e35-6616-45db-8948-5de86d0ff7e3").unwrap(),
    ];

    let node1_cfg = btreemap! {
        server_api_1_P1In as usize => btreeset!{
            RelCfg::Source(Source::File(input1.to_path_buf())),
        },
        server_api_1_P1Out as usize => btreeset!{
            RelCfg::Output(uuids[2], server_api_3_P1Out as usize),
        },
    };
    let node2_cfg = btreemap! {
        server_api_2_P2In as usize => btreeset!{
            RelCfg::Source(Source::File(input2.to_path_buf())),
        },
        server_api_2_P2Out as usize => btreeset!{
            RelCfg::Output(uuids[2], server_api_3_P2Out as usize),
        },
    };
    let node3_cfg = btreemap! {
        server_api_3_P1Out as usize => btreeset!{
            RelCfg::Input(server_api_1_P1Out as usize)
        },
        server_api_3_P2Out as usize => btreeset!{
            RelCfg::Input(server_api_2_P2Out as usize)
        },
        server_api_3_P3Out as usize => btreeset!{
            RelCfg::Sink(Sink::File(output.to_path_buf())),
        },
    };

    let sys_cfg = btreemap! {
        uuids[0] => node1_cfg,
        uuids[1] => node2_cfg,
        uuids[2] => node3_cfg,
    };

    sys_cfg
}
