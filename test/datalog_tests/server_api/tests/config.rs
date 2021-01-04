use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use maplit::btreeset;

use tempfile::NamedTempFile;
use test_env_log::test;

use distributed_datalog::await_expected;
use distributed_datalog::instantiate;
use distributed_datalog::simple_assign;
use distributed_datalog::Addr;
use distributed_datalog::Member;

use server_api_ddlog::api::HDDlog;
use server_api_ddlog::{DDlogConverter, UpdateSerializer};
use server_api_test::config;

/// Test delta retrieval in the face of two concurrent transactions over
/// a TCP channel.
#[test]
fn instantiate_configuration_end_to_end() -> Result<(), String> {
    const SERVER_API_1_P1IN: &'static [u8] = include_bytes!("../data/server_api_1_p1in.dat");
    const SERVER_API_2_P2IN: &'static [u8] = include_bytes!("../data/server_api_2_p2in.dat");
    const SERVER_API_3_P3OUT: &'static str =
        include_str!("../data/server_api_3_p3out.dump.expected");

    let mut file1 = NamedTempFile::new().unwrap();
    file1.write_all(SERVER_API_1_P1IN).unwrap();
    let path1 = file1.into_temp_path();

    let mut file2 = NamedTempFile::new().unwrap();
    file2.write_all(SERVER_API_2_P2IN).unwrap();
    let path2 = file2.into_temp_path();

    let (mut file3, path3) = NamedTempFile::new().unwrap().into_parts();

    // TODO: We run risk of port collisions here. The range is chosen to
    //       be unlikely to be used by the system for ephemeral ports,
    //       but that is not enough in the long term. We likely need
    //       some form of registry.
    let node1 = Addr::Ip("127.0.0.1:5000".parse().unwrap());
    let node2 = Addr::Ip("127.0.0.1:5001".parse().unwrap());
    let node3 = Addr::Ip("127.0.0.1:5002".parse().unwrap());

    let members = btreeset! {
        Member::new(node1.clone()),
        Member::new(node2.clone()),
        Member::new(node3.clone()),
    };

    let sys_cfg = config(path1.as_ref(), path2.as_ref(), path3.as_ref());
    let assignment = simple_assign(sys_cfg.keys(), members.iter()).unwrap();
    let _realization1 = instantiate::<HDDlog, DDlogConverter, UpdateSerializer>(sys_cfg.clone(), &node1, &assignment, HDDlog::run(1, false).unwrap().0).unwrap();
    let _realization2 = instantiate::<HDDlog, DDlogConverter, UpdateSerializer>(sys_cfg.clone(), &node2, &assignment, HDDlog::run(1, false).unwrap().0).unwrap();
    let _realization3 = instantiate::<HDDlog, DDlogConverter, UpdateSerializer>(sys_cfg.clone(), &node3, &assignment, HDDlog::run(1, false).unwrap().0).unwrap();

    await_expected(move || {
        let mut string = String::new();
        let _ = file3.seek(SeekFrom::Start(0)).unwrap();
        let _ = file3.read_to_string(&mut string).unwrap();

        assert_eq!(string, SERVER_API_3_P3OUT);
    });

    Ok(())
}
