use maplit::btreemap;
use maplit::btreeset;

use uuid::Uuid;

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
fn blah() -> Result<(), String> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let node1 = Addr::Ip("127.0.0.1:2000".parse().unwrap());
    let node2 = Addr::Ip("127.0.0.1:2001".parse().unwrap());
    let node3 = Addr::Ip("127.0.0.1:2002".parse().unwrap());

    let node1_cfg = btreemap! {
        server_api_1_P1In as usize => btreeset!{
            RelCfg::Source(Source::File("/tmp/node1.dat".into())),
        },
    };
    let node2_cfg = btreemap! {
        server_api_2_P2In as usize => btreeset!{
            RelCfg::Source(Source::File("/tmp/node2.dat".into())),
        },
    };
    let node3_cfg = btreemap! {
        server_api_3_P1Out as usize => btreeset!{
            RelCfg::Input(btreeset!{
                server_api_1_P1Out as usize,
            })
        },
        server_api_3_P2Out as usize => btreeset!{
            RelCfg::Input(btreeset!{
                server_api_2_P2Out as usize,
            })
        },
        server_api_3_P3Out as usize => btreeset!{
            RelCfg::Sink(Sink::File("/tmp/node3.dump".into())),
        },
    };

    let sys_cfg = btreemap! {
        uuid1 => node1_cfg,
        uuid2 => node2_cfg,
        uuid3 => node3_cfg,
    };

    let members = btreeset! {
        Member::new(node1.clone()),
        Member::new(node2.clone()),
        Member::new(node3.clone()),
    };

    let assignment = simple_assign(sys_cfg.keys(), members.iter()).unwrap();
    let _config1 = instantiate::<HDDlog>(sys_cfg.clone(), &node1, &assignment).unwrap();
    let _config2 = instantiate::<HDDlog>(sys_cfg.clone(), &node2, &assignment).unwrap();
    let _config3 = instantiate::<HDDlog>(sys_cfg.clone(), &node3, &assignment).unwrap();
    Ok(())
}
