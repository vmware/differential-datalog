use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::ops::Deref;

use maplit::btreemap;
use maplit::btreeset;

use tempfile::NamedTempFile;
use test_env_log::test;

use uuid::Uuid;

use distributed_datalog::await_expected;
use distributed_datalog::instantiate;
use distributed_datalog::simple_assign;
use distributed_datalog::Addr;
use distributed_datalog::Member;
use distributed_datalog::RelCfg;
use distributed_datalog::Sink;
use distributed_datalog::Source;

use server_api_ddlog::api::HDDlog;
use server_api_ddlog::Relations::server_api_1_P1In;
use server_api_ddlog::Relations::server_api_1_P1Out;
use server_api_ddlog::Relations::server_api_2_P2In;
use server_api_ddlog::Relations::server_api_2_P2Out;
use server_api_ddlog::Relations::server_api_3_P1Out;
use server_api_ddlog::Relations::server_api_3_P2Out;
use server_api_ddlog::Relations::server_api_3_P3Out;

/// Test delta retrieval in the face of two concurrent transactions over
/// a TCP channel.
#[test]
fn instantiate_configuration_end_to_end() -> Result<(), String> {
    const SERVER_API_1_P1IN: &'static [u8] = include_bytes!("server_api_1_p1in.dat");
    const SERVER_API_2_P2IN: &'static [u8] = include_bytes!("server_api_2_p2in.dat");
    const SERVER_API_3_P3OUT: &'static str = include_str!("server_api_3_p3out.dump.expected");

    let mut file1 = NamedTempFile::new().unwrap();
    file1.write_all(SERVER_API_1_P1IN).unwrap();
    let path1 = file1.into_temp_path();

    let mut file2 = NamedTempFile::new().unwrap();
    file2.write_all(SERVER_API_2_P2IN).unwrap();
    let path2 = file2.into_temp_path();

    let (mut file3, path3) = NamedTempFile::new().unwrap().into_parts();

    // We require UUIDs in ascending order to be able to create a stable
    // assignment.
    let mut uuids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    uuids.sort();

    // TODO: We run risk of port collisions here. The range is chosen to
    //       be unlikely to be used by the system for ephemeral ports,
    //       but that is not enough in the long term. We likely need
    //       some form of registry.
    let node1 = Addr::Ip("127.0.0.1:5000".parse().unwrap());
    let node2 = Addr::Ip("127.0.0.1:5001".parse().unwrap());
    let node3 = Addr::Ip("127.0.0.1:5002".parse().unwrap());

    let node1_cfg = btreemap! {
        server_api_1_P1In as usize => btreeset!{
            RelCfg::Source(Source::File(path1.deref().into())),
        },
        server_api_1_P1Out as usize => btreeset!{
            RelCfg::Output(uuids[2], server_api_3_P1Out as usize),
        },
    };
    let node2_cfg = btreemap! {
        server_api_2_P2In as usize => btreeset!{
            RelCfg::Source(Source::File(path2.deref().into())),
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
            RelCfg::Sink(Sink::File(path3.deref().into())),
        },
    };

    let sys_cfg = btreemap! {
        uuids[0] => node1_cfg,
        uuids[1] => node2_cfg,
        uuids[2] => node3_cfg,
    };

    let members = btreeset! {
        Member::new(node1.clone()),
        Member::new(node2.clone()),
        Member::new(node3.clone()),
    };

    let assignment = simple_assign(sys_cfg.keys(), members.iter()).unwrap();

    // TODO: Because of TCP senders synchronously connecting to TCP
    //       receivers we instantiate the nodes in "reverse", meaning
    //       with the ones feeding the others first. That's only
    //       possible while we don't have any loops in the
    //       configuration. In the future we may want to make
    //       `TcpSender` more clever and allowing it to buffer updates,
    //       or something along those lines.
    let _realization3 = instantiate::<HDDlog>(sys_cfg.clone(), &node3, &assignment).unwrap();
    let _realization2 = instantiate::<HDDlog>(sys_cfg.clone(), &node2, &assignment).unwrap();
    let _realization1 = instantiate::<HDDlog>(sys_cfg.clone(), &node1, &assignment).unwrap();

    await_expected(move || {
        let mut string = String::new();
        let _ = file3.seek(SeekFrom::Start(0)).unwrap();
        let _ = file3.read_to_string(&mut string).unwrap();

        assert_eq!(string, SERVER_API_3_P3OUT);
    });

    Ok(())
}
