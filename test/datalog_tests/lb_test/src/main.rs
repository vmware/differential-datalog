use lb_ddlog::api::HDDlog;

use lb_ddlog::Relations;

use lb_ddlog::relid2name;

use lb_ddlog::typedefs::*;

use differential_datalog::{D3log, DDlog, DDlogDynamic};
use differential_datalog::DeltaMap;
use differential_datalog::ddval::DDValue;
use differential_datalog::ddval::DDValConvert;
use differential_datalog::program::RelId;
use differential_datalog::program::Update;

fn main() -> Result<(), String> {

    let (hddlog, _) = HDDlog::run(1, false)?;

    hddlog.transaction_start()?;

    let updates = vec![
        Update::Insert {
            relid: Relations::Data as RelId,
            v: Data {
                dat: 1,
            }
            .into_ddvalue(),
        },
        Update::Insert {
            relid: Relations::Data as RelId,
            v: Data {
                dat: 2,
            }
            .into_ddvalue(),
        },
        Update::Insert {
            relid: Relations::Workers as RelId,
            v: Workers {
                id: 100,
            }
            .into_ddvalue(),
        },
        Update::Insert {
            relid: Relations::Workers as RelId,
            v: Workers {
                id: 200,
            }
            .into_ddvalue(),
        },
    ];
    hddlog.apply_updates(&mut updates.into_iter())?;

    let delta = hddlog.transaction_commit_dump_changes()?;

    println!("\nState after transaction 1");
    dump_delta(&hddlog, delta);

    hddlog.transaction_start()?;

    let updates = vec![
        Update::Insert {
            relid: Relations::Workers as RelId,
            v: Workers {
                id: 300,
            }
            .into_ddvalue(),
        },
    ];
    hddlog.apply_updates(&mut updates.into_iter())?;

    let delta = hddlog.transaction_commit_dump_changes()?;

    println!("\nState after transaction 2");
    dump_delta(&hddlog, delta);

    hddlog.stop().unwrap();
    Ok(())
}

fn dump_delta(hddlog: &HDDlog, delta: DeltaMap<DDValue>) {
    for (rel, changes) in delta.into_iter() {
        println!("Changes to relation {}", relid2name(rel).unwrap());
        for (val, weight) in changes.into_iter() {
            match hddlog.d3log_localize_val(rel, val) {
                Ok((loc_id, _in_rel, inner_val)) => println!("{} @ {:?} {:+}", inner_val, loc_id, weight),
                Err(val) => println!("{} {:+}", val, weight)
            }
        }
    }
}
